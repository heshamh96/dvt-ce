"""DVT InitTask — replaces dbt's interactive init with a non-interactive flow.

Key differences from upstream dbt init:
1. No interactive adapter prompt — users configure profiles.yml manually
2. 'dvt init' (no arg) initializes in the current directory using cwd name
3. 'dvt init <name>' creates a subdirectory (like dbt)
4. Always writes commented profile templates to all 3 config files
5. Guides user to edit configs and run 'dvt sync' to install adapters

This lives in dvt_tasks/ (not task/) so the upstream dbt init.py can stay
untouched for clean rebases.
"""

import os
import shutil
from pathlib import Path

import click
import yaml

import dbt_common.clients.system
from dvt.config.user_config import (
    append_profile_to_buckets_yml,
    append_profile_to_computes_yml,
    append_profile_to_profiles_yml,
    create_default_buckets_yml,
    create_default_computes_yml,
    create_default_profiles_yml,
    create_dvt_data_dir,
    init_mdm_db,
)
from dvt.constants import DBT_PROJECT_FILE_NAME, DVT_PROJECT_FILE_NAME
from dvt.contracts.util import Identifier as ProjectName
from dvt.events.types import ConfigFolderDirectory
from dvt.flags import get_flags
from dvt.task.base import BaseTask
from dbt_common.events.functions import fire_event

# Files to skip when copying starter project
IGNORE_FILES = ["__init__.py", "__pycache__"]

DISCORD_URL = "https://discord.gg/UjQcxJXAQp"
ADAPTERS_URL = "https://docs.getdbt.com/docs/available-adapters"
DVT_REPO_URL = "https://github.com/heshamh96/dvt-ce"


class DvtInitTask(BaseTask):
    """DVT-specific init task. Non-interactive, federation-aware."""

    def _get_starter_project_dir(self) -> str:
        """Get the path to the starter project template."""
        from dvt.include.starter_project import (
            PACKAGE_PATH as starter_project_directory,
        )

        return starter_project_directory

    def _copy_starter_files_to(self, target_dir: Path) -> None:
        """Copy starter project files into target_dir, skipping files that already exist.

        This is used when initializing in the current directory — we don't want
        to overwrite the user's existing files (like pyproject.toml, .git, etc.),
        but we do want to fill in any missing project structure files.
        """
        starter_dir = Path(self._get_starter_project_dir())

        for item in starter_dir.rglob("*"):
            # Skip __pycache__ and __init__.py
            if any(ignore in str(item) for ignore in IGNORE_FILES):
                continue

            relative = item.relative_to(starter_dir)
            destination = target_dir / relative

            if item.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
            elif item.is_file():
                if not destination.exists():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(item), str(destination))

    def _copy_starter_to_subdir(self, project_name: str) -> None:
        """Copy starter project into a new subdirectory (traditional dbt init behavior)."""
        starter_dir = self._get_starter_project_dir()
        shutil.copytree(
            starter_dir,
            project_name,
            ignore=shutil.ignore_patterns(*IGNORE_FILES),
        )

    def _render_project_yml(
        self, project_dir: Path, project_name: str, profile_name: str
    ) -> None:
        """Replace {project_name} and {profile_name} placeholders in dbt_project.yml."""
        project_yml = project_dir / DBT_PROJECT_FILE_NAME
        if not project_yml.exists():
            return
        with open(project_yml, "r") as f:
            content = f.read()
        # Only render if placeholders are present (skip if already rendered)
        if "{project_name}" in content or "{profile_name}" in content:
            content = content.format(
                project_name=project_name, profile_name=profile_name
            )
            with open(project_yml, "w") as f:
                f.write(content)

    def _is_in_project_directory(self) -> bool:
        """Check if the current directory contains a dbt/dvt project file."""
        cwd = Path.cwd()
        return (cwd / DBT_PROJECT_FILE_NAME).is_file() or (
            cwd / DVT_PROJECT_FILE_NAME
        ).is_file()

    def _get_profile_name_from_project(self) -> str:
        """Read profile name from dbt_project.yml in the current directory."""
        for fname in [DBT_PROJECT_FILE_NAME, DVT_PROJECT_FILE_NAME]:
            project_file = Path.cwd() / fname
            if project_file.exists():
                with open(project_file) as f:
                    data = yaml.safe_load(f)
                if data and "profile" in data:
                    return data["profile"]
        raise click.ClickException(
            f"Could not find 'profile' key in {DBT_PROJECT_FILE_NAME}"
        )

    def _sanitize_project_name(self, name: str) -> str:
        """Sanitize a directory name into a valid project/profile name.

        Replaces hyphens and spaces with underscores, strips invalid chars.
        """
        import re

        # Replace hyphens and spaces with underscores
        name = re.sub(r"[-\s]+", "_", name)
        # Strip anything that isn't alphanumeric or underscore
        name = re.sub(r"[^a-zA-Z0-9_]", "", name)
        # Ensure it doesn't start with a digit
        if name and name[0].isdigit():
            name = "_" + name
        return name or "my_project"

    def _create_dvt_home(self, profiles_dir: str) -> None:
        """Create ~/.dvt/ directory and all config file headers.

        Fires ConfigFolderDirectory event only once when the directory is created.
        Individual config file creation is logged separately.
        """
        profiles_path = Path(profiles_dir)

        # Create the directory
        if not profiles_path.exists():
            fire_event(ConfigFolderDirectory(dir=str(profiles_dir)))
            dbt_common.clients.system.make_directory(profiles_dir)

        dvt_home = profiles_path.resolve()

        # Create config file headers (only if they don't exist)
        profiles_yml = dvt_home / "profiles.yml"
        if create_default_profiles_yml(profiles_yml):
            click.echo(f"  Created {profiles_yml}")

        computes_yml = dvt_home / "computes.yml"
        if create_default_computes_yml(computes_yml):
            click.echo(f"  Created {computes_yml}")

        buckets_yml = dvt_home / "buckets.yml"
        if create_default_buckets_yml(buckets_yml):
            click.echo(f"  Created {buckets_yml}")

        # Create data directory and MDM database
        create_dvt_data_dir(str(dvt_home))
        init_mdm_db(str(dvt_home))

    def _print_success(
        self, project_name: str, profile_name: str, profiles_dir: str
    ) -> None:
        """Print the success message with next steps."""
        dvt_home = Path(profiles_dir).resolve()
        click.echo(f"""
Your DVT project "{project_name}" has been initialized.

Next steps:
  1. Edit {dvt_home}/profiles.yml — add your database connection
     (see {ADAPTERS_URL} for supported database adapters)
  2. Edit {dvt_home}/computes.yml — configure your Spark compute engine
  3. Run 'dvt sync' to install adapters, PySpark, and JDBC drivers
  4. Run 'dvt debug' to verify all connections
  5. Run 'dvt run' to execute your models

Documentation: {DVT_REPO_URL}
Need help? {DISCORD_URL}
""")

    def run(self):
        """Entry point for the DVT init task."""
        profiles_dir = get_flags().PROFILES_DIR

        # Step 1: Create ~/.dvt/ and config file headers
        self._create_dvt_home(profiles_dir)

        project_name_arg = self.args.project_name

        if project_name_arg:
            # --- dvt init <project_name> ---
            # Create a new subdirectory with the starter project
            project_name = project_name_arg
            profile_name = project_name

            # Validate project name
            if not ProjectName.is_valid(project_name):
                sanitized = self._sanitize_project_name(project_name)
                raise click.ClickException(
                    f"'{project_name}' is not a valid project name "
                    f"(letters, digits, underscore only). Try: {sanitized}"
                )

            project_path = Path(project_name)
            if project_path.exists():
                click.echo(f"A project called {project_name} already exists here.")
                return

            self._copy_starter_to_subdir(project_name)
            self._render_project_yml(project_path, project_name, profile_name)
            click.echo(f"  Created project directory: {project_path.resolve()}")

        else:
            # --- dvt init (no arg) ---
            # Initialize in the current directory
            if self._is_in_project_directory():
                # Already a project — just read the profile name and ensure configs exist
                profile_name = self._get_profile_name_from_project()
                project_name = profile_name
                click.echo(f"  Found existing project with profile '{profile_name}'")
            else:
                # New project in current directory
                raw_name = Path.cwd().name
                project_name = self._sanitize_project_name(raw_name)
                profile_name = project_name

                if not ProjectName.is_valid(project_name):
                    raise click.ClickException(
                        f"Could not derive a valid project name from directory '{raw_name}'. "
                        f"Use 'dvt init <project_name>' to specify one explicitly."
                    )

                # Copy starter files into cwd (skip existing)
                self._copy_starter_files_to(Path.cwd())
                self._render_project_yml(Path.cwd(), project_name, profile_name)
                click.echo(
                    f"  Initialized project '{project_name}' in current directory"
                )

        # Step 3: Append profile to all 3 config files (if not already there)
        profiles_dir_str = str(profiles_dir)
        if append_profile_to_profiles_yml(profile_name, profiles_dir_str):
            click.echo(f"  Added profile '{profile_name}' to profiles.yml")
        if append_profile_to_computes_yml(profile_name, profiles_dir_str):
            click.echo(f"  Added profile '{profile_name}' to computes.yml")
        if append_profile_to_buckets_yml(profile_name, profiles_dir_str):
            click.echo(f"  Added profile '{profile_name}' to buckets.yml")

        # Step 4: Print success and next steps
        self._print_success(project_name, profile_name, profiles_dir_str)
