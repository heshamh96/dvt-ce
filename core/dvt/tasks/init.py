"""
DVT Init Task — initialize a new DVT project.

Replaces dbt's InitTask with DVT-specific behavior:
- Defaults project name to current directory name
- Creates ~/.dvt/ and profiles.yml automatically
- Doesn't crash on missing adapter drivers (defers to dvt sync)
- References DVT docs/Discord instead of dbt
"""

import os
import re
import shutil
from pathlib import Path
from typing import Optional

import click
import yaml

from dbt.task.init import InitTask

# Supported adapter types for profile templates
ADAPTER_TEMPLATES = {
    "postgres": {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "dbname": "postgres",
        "schema": "public",
        "threads": 4,
    },
    "snowflake": {
        "type": "snowflake",
        "account": "your-account",
        "user": "your-user",
        "password": "your-password",
        "database": "YOUR_DATABASE",
        "schema": "PUBLIC",
        "warehouse": "COMPUTE_WH",
        "threads": 4,
    },
    "mysql": {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "mydb",
        "schema": "mydb",
        "threads": 4,
    },
    "bigquery": {
        "type": "bigquery",
        "method": "oauth",
        "project": "your-gcp-project",
        "dataset": "your_dataset",
        "threads": 4,
    },
    "databricks": {
        "type": "databricks",
        "host": "your-workspace.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/your-warehouse-id",
        "token": "your-token",
        "catalog": "main",
        "schema": "default",
        "threads": 4,
    },
    "redshift": {
        "type": "redshift",
        "host": "your-cluster.region.redshift.amazonaws.com",
        "port": 5439,
        "user": "your-user",
        "password": "your-password",
        "dbname": "dev",
        "schema": "public",
        "threads": 4,
    },
    "sqlserver": {
        "type": "sqlserver",
        "host": "localhost",
        "port": 1433,
        "user": "sa",
        "password": "your-password",
        "database": "master",
        "schema": "dbo",
        "threads": 4,
    },
    "oracle": {
        "type": "oracle",
        "host": "localhost",
        "port": 1521,
        "user": "system",
        "password": "your-password",
        "service": "XEPDB1",
        "schema": "SYSTEM",
        "threads": 4,
    },
    "mariadb": {
        "type": "mariadb",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "mydb",
        "schema": "mydb",
        "threads": 4,
    },
    "duckdb": {
        "type": "duckdb",
        "path": "dev.duckdb",
        "threads": 4,
    },
    "spark": {
        "type": "spark",
        "method": "thrift",
        "host": "localhost",
        "port": 10000,
        "schema": "default",
        "threads": 4,
    },
    "fabric": {
        "type": "fabric",
        "driver": "ODBC Driver 18 for SQL Server",
        "host": "your-workspace.datawarehouse.fabric.microsoft.com",
        "port": 1433,
        "database": "your-database",
        "schema": "dbo",
        "threads": 4,
    },
    "mysql5": {
        "type": "mysql5",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "mydb",
        "schema": "mydb",
        "threads": 4,
    },
}

ADAPTER_LIST = [
    "bigquery", "databricks", "duckdb", "fabric", "mariadb",
    "mysql", "mysql5", "oracle", "postgres", "redshift",
    "snowflake", "spark", "sqlserver",
]

SAMPLE_PROFILES = """\
# DVT Profiles Configuration
# Documentation: https://github.com/heshamh96/dvt-ce
#
# This file defines your database connections.
# DVT looks for profiles in: ~/.dvt/ (first), then ~/.dbt/ (fallback)
#
# Run 'dvt sync' after editing to install the required database drivers.
# Run 'dvt debug' to test all connections.

{project_name}:
  target: dev
  outputs:
    dev:
{adapter_config}
"""


def _sanitize_project_name(name: str) -> str:
    """Sanitize a directory name into a valid dbt project name."""
    name = name.replace("-", "_").replace(" ", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    if name and name[0].isdigit():
        name = "_" + name
    return name or "my_project"


def _format_adapter_config(adapter_type: str, indent: int = 6) -> str:
    """Format an adapter template as YAML with the given indentation."""
    template = ADAPTER_TEMPLATES.get(adapter_type, ADAPTER_TEMPLATES["postgres"])
    lines = []
    for key, value in template.items():
        if isinstance(value, str):
            lines.append(f"{' ' * indent}{key}: {value}")
        else:
            lines.append(f"{' ' * indent}{key}: {value}")
    return "\n".join(lines)


def _dvt_profiles_dir() -> str:
    """Return ~/.dvt as the DVT profiles directory."""
    return str(Path.home() / ".dvt")


def _ensure_dvt_dir() -> str:
    """Create ~/.dvt/ if it doesn't exist. Returns the path."""
    dvt_dir = Path.home() / ".dvt"
    dvt_dir.mkdir(exist_ok=True)
    return str(dvt_dir)


class DvtInitTask:
    """Initialize a new DVT project."""

    def __init__(self, flags):
        self.args = flags

    def run(self):
        project_name = getattr(self.args, "PROJECT_NAME", None) or getattr(
            self.args, "project_name", None
        )
        skip_profile = getattr(self.args, "SKIP_PROFILE_SETUP", False)

        # Ensure ~/.dvt/ exists
        dvt_dir = _ensure_dvt_dir()

        # Check if we're inside an existing project
        in_project = Path("dbt_project.yml").exists()

        if in_project:
            # Inside existing project — just set up profile
            project_name = self._read_project_name()
            click.echo(f"\n  Found existing project: {project_name}")
            self._ensure_profiles(dvt_dir, project_name, skip_profile)
            return True

        # Not in a project — create one
        if not project_name:
            # Default to current directory name
            cwd_name = _sanitize_project_name(Path.cwd().name)
            project_name = click.prompt(
                "Enter a name for your project",
                default=cwd_name,
            )
            project_name = _sanitize_project_name(project_name)

        # Validate
        project_name = _sanitize_project_name(project_name)

        # Create project scaffold
        self._create_project(project_name)

        # Set up profiles
        self._ensure_profiles(dvt_dir, project_name, skip_profile)

        # Print next steps
        self._print_next_steps(project_name, dvt_dir)

        return True

    def interpret_results(self, results):
        return bool(results)

    def _read_project_name(self) -> str:
        """Read project name from dbt_project.yml."""
        try:
            with open("dbt_project.yml") as f:
                data = yaml.safe_load(f)
            return data.get("name", "my_project")
        except Exception:
            return "my_project"

    def _create_project(self, project_name: str) -> None:
        """Create a new DVT project scaffold."""
        project_dir = Path(project_name)

        if project_dir.exists() and any(project_dir.iterdir()):
            click.echo(
                f"\n  Directory '{project_name}' already exists and is not empty."
            )
            click.echo("  Initializing inside current directory instead.\n")
            project_dir = Path(".")
            # Write dbt_project.yml if it doesn't exist
            if not (project_dir / "dbt_project.yml").exists():
                self._write_dbt_project_yml(project_dir, project_name)
                self._create_dirs(project_dir)
        else:
            project_dir.mkdir(exist_ok=True)
            self._write_dbt_project_yml(project_dir, project_name)
            self._create_dirs(project_dir)

        click.echo(f'  Created project "{project_name}"')

    def _write_dbt_project_yml(self, project_dir: Path, name: str) -> None:
        """Write a dbt_project.yml file."""
        content = f"""\
# DVT Project Configuration
# Documentation: https://github.com/heshamh96/dvt-ce

name: '{name}'
version: '1.0.0'

profile: '{name}'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"
  - ".dvt"
"""
        (project_dir / "dbt_project.yml").write_text(content)

    def _create_dirs(self, project_dir: Path) -> None:
        """Create standard project directories."""
        for d in [
            "models", "analyses", "tests", "seeds",
            "macros", "snapshots",
        ]:
            (project_dir / d).mkdir(exist_ok=True)

        # Create a sample model
        models_dir = project_dir / "models"
        example_dir = models_dir / "example"
        example_dir.mkdir(exist_ok=True)

        (example_dir / "my_first_model.sql").write_text(
            "-- Your first DVT model\n"
            "-- Run with: dvt run\n\n"
            "select 1 as id, 'hello DVT' as message\n"
        )

        (example_dir / "schema.yml").write_text(
            "version: 2\n\n"
            "models:\n"
            "  - name: my_first_model\n"
            '    description: "A starter DVT model"\n'
            "    columns:\n"
            "      - name: id\n"
            "        tests:\n"
            "          - unique\n"
            "          - not_null\n"
        )

    def _ensure_profiles(
        self, dvt_dir: str, project_name: str, skip_profile: bool
    ) -> None:
        """Ensure ~/.dvt/profiles.yml exists with the project's profile."""
        profiles_path = Path(dvt_dir) / "profiles.yml"

        if profiles_path.exists():
            # Check if this project's profile already exists
            try:
                with open(profiles_path) as f:
                    existing = yaml.safe_load(f) or {}
                if project_name in existing:
                    click.echo(
                        f"  Profile '{project_name}' already exists in {profiles_path}"
                    )
                    return
            except Exception:
                pass

        # Determine adapter type
        adapter_type = "postgres"  # default
        if not skip_profile:
            adapter_type = self._prompt_adapter()

        # Generate profile config
        adapter_config = _format_adapter_config(adapter_type)
        profile_content = SAMPLE_PROFILES.format(
            project_name=project_name,
            adapter_config=adapter_config,
        )

        if profiles_path.exists():
            # Append to existing profiles.yml
            with open(profiles_path, "a") as f:
                f.write("\n" + profile_content)
            click.echo(f"  Added profile '{project_name}' to {profiles_path}")
        else:
            profiles_path.write_text(profile_content)
            click.echo(f"  Created {profiles_path}")

        # Set DBT_PROFILES_DIR so dbt finds it
        os.environ["DBT_PROFILES_DIR"] = dvt_dir

    def _prompt_adapter(self) -> str:
        """Prompt user to select a database adapter."""
        click.echo("\n  Which database would you like to use?")
        for i, adapter in enumerate(ADAPTER_LIST, 1):
            click.echo(f"  [{i}] {adapter}")

        click.echo()
        while True:
            choice = click.prompt(
                "  Enter a number", type=int, default=9
            )  # default=postgres
            if 1 <= choice <= len(ADAPTER_LIST):
                selected = ADAPTER_LIST[choice - 1]
                click.echo(f"  Selected: {selected}")
                return selected
            click.echo(f"  Please enter a number between 1 and {len(ADAPTER_LIST)}")

    def _print_next_steps(self, project_name: str, dvt_dir: str) -> None:
        """Print DVT-specific next steps."""
        click.echo(f"""
  Your DVT project "{project_name}" is ready!

  Next steps:
    1. Edit {dvt_dir}/profiles.yml — configure your database connection
    2. Run 'dvt sync' to install database drivers
    3. Run 'dvt debug' to verify your connection
    4. Run 'dvt run' to execute your models

  Documentation: https://github.com/heshamh96/dvt-ce
  Community:     https://discord.gg/UjQcxJXAQp
""")
