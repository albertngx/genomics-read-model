"""``cdc`` CLI entrypoint.

A thin Typer app so the console-script wiring is proven now; real subcommands are
registered by later epics (verifier, replay, demos live in ``cdc_pipeline.tools``).
"""

from __future__ import annotations

import typer

from cdc_pipeline import __version__

app = typer.Typer(help="cdc-read-model operational CLI.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Root callback — keeps ``cdc`` a command group so subcommands stay explicit
    (without it, a single-command Typer app collapses ``version`` into the root)."""


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    app()
