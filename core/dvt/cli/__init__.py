"""
DVT CLI package.

Provides resilient CLI entry point that works even when dbt dependencies
are broken. In that case, only 'dvt sync' is available to fix the environment.
"""

import click


def cli():
    """DVT CLI entry point — resilient to broken dbt imports."""
    try:
        from dvt.cli.main import cli as full_cli

        full_cli(standalone_mode=True)
    except (ImportError, Exception) as e:
        _broken_cli(str(e))


def _broken_cli(error_msg: str):
    """Minimal CLI when dbt dependencies are broken."""

    @click.group(
        invoke_without_command=True,
        no_args_is_help=True,
    )
    @click.pass_context
    def broken(ctx):
        """DVT — environment needs repair. Run 'dvt sync' to fix."""
        if ctx.invoked_subcommand is None:
            click.echo(f"\nERROR: dbt dependencies are broken:")
            click.echo(f"  {error_msg}")
            click.echo(
                "\nRun 'dvt sync --profiles-dir <path>' to fix your environment.\n"
            )

    @broken.command("sync")
    @click.option(
        "--profiles-dir",
        default=None,
        type=click.Path(),
        help="Directory containing profiles.yml.",
    )
    @click.option(
        "--skip-test", is_flag=True, default=False, help="Skip connection testing."
    )
    @click.option(
        "--dry-run", is_flag=True, default=False, help="Show what would be installed."
    )
    def sync(**kwargs):
        """Sync DVT environment: fix dependencies, install adapters, verify Sling."""
        from dvt.tasks.sync import DvtSyncTask

        class _Flags:
            PROFILES_DIR = kwargs.get("profiles_dir")

        task = DvtSyncTask(_Flags(), kwargs)
        results = task.run()
        if not results or not results.get("success"):
            raise SystemExit(1)

    broken(standalone_mode=True)
