"""CLI for DAX-to-Metric View Translator."""

from __future__ import annotations

import json
import sys

import click

from .models import DaxModel


@click.group()
def cli():
    """DAX-to-Databricks Metric View Translator."""
    pass


@cli.command()
@click.option("--input", "-i", "input_file", required=True, help="Path to DAX model JSON file")
@click.option("--output", "-o", "output_file", help="Output file for generated SQL (default: stdout)")
@click.option("--model", "-m", "claude_model", help="Claude model to use")
@click.option("--show-yaml", is_flag=True, help="Show YAML body only (no CREATE VIEW wrapper)")
def translate(input_file: str, output_file: str | None, claude_model: str | None, show_yaml: bool):
    """Translate a DAX model JSON file into a Databricks Metric View."""
    from .translator import translate as do_translate

    with open(input_file) as f:
        data = json.load(f)

    dax_model = DaxModel(**data)
    click.echo(f"Translating DAX model: {dax_model.name} ({len(dax_model.measures)} measures)")

    result = do_translate(dax_model, model=claude_model)

    # Show warnings
    if result.warnings:
        click.echo(click.style(f"\n⚠ {len(result.warnings)} warning(s):", fg="yellow"))
        for w in result.warnings:
            click.echo(f"  - {w.measure_name}: {w.message}")

    # Output
    output = result.yaml_body if show_yaml else result.sql
    if output_file:
        with open(output_file, "w") as f:
            f.write(output)
        click.echo(f"\nWritten to {output_file}")
    else:
        click.echo(f"\n{'─' * 60}")
        click.echo(output)

    click.echo(f"\nStatus: {result.status.value} | Version: {result.version} | "
               f"Dims: {len(result.dimensions)} | Measures: {len(result.measures)}")


@cli.command("test-all")
@click.option("--profile", "-p", default="DEFAULT", help="Databricks CLI profile")
@click.option("--catalog", default="main", help="Target catalog")
@click.option("--schema", default="dax_translator_test", help="Target schema")
@click.option("--create-data", is_flag=True, help="Create test data tables first")
@click.option("--cleanup", is_flag=True, help="Clean up test views after running")
@click.option("--level", type=int, help="Run only a specific level (1-4)")
def test_all(profile: str, catalog: str, schema: str, create_data: bool, cleanup: bool, level: int | None):
    """Translate and deploy all test DAX models, then validate on Databricks."""
    from .deployer import cleanup_test_views, create_test_data, deploy_metric_view, get_client
    from .translator import translate as do_translate
    from .validator import validate_all

    client = get_client(profile)

    if create_data:
        click.echo("Creating test data...")
        create_test_data(client, catalog, schema)
        click.echo("Done.\n")

    levels = {
        1: ("level1_simple", "dax_translator.dax_samples.level1_simple"),
        2: ("level2_medium", "dax_translator.dax_samples.level2_medium"),
        3: ("level3_complex", "dax_translator.dax_samples.level3_complex"),
        4: ("level4_very_complex", "dax_translator.dax_samples.level4_very_complex"),
    }

    if level:
        levels = {level: levels[level]}

    from importlib import import_module

    all_passed = True

    for lvl_num, (lvl_name, module_path) in sorted(levels.items()):
        click.echo(f"\n{'═' * 60}")
        click.echo(f"Level {lvl_num}: {lvl_name}")
        click.echo(f"{'═' * 60}")

        mod = import_module(module_path)
        dax_model = mod.get_model()
        dax_model.catalog = catalog
        dax_model.schema_name = schema

        # Translate
        click.echo(f"  Translating {len(dax_model.measures)} measures...")
        try:
            result = do_translate(dax_model)
        except Exception as e:
            click.echo(click.style(f"  TRANSLATION FAILED: {e}", fg="red"))
            all_passed = False
            continue

        click.echo(f"  Status: {result.status.value} | "
                    f"Dims: {len(result.dimensions)} | Measures: {len(result.measures)}")

        if result.warnings:
            click.echo(click.style(f"  Warnings ({len(result.warnings)}):", fg="yellow"))
            for w in result.warnings:
                click.echo(f"    - {w.measure_name}: {w.message}")

        # Deploy
        click.echo("  Deploying metric view...")
        try:
            deploy_metric_view(client, result.sql)
            click.echo(click.style("  Deployed successfully.", fg="green"))
        except Exception as e:
            click.echo(click.style(f"  DEPLOY FAILED: {e}", fg="red"))
            all_passed = False
            continue

        # Validate
        click.echo("  Validating measures...")
        try:
            validation_results = validate_all(result, catalog, schema, profile)
            for vr in validation_results:
                if vr.passed:
                    click.echo(click.style(f"    ✓ {vr.case.measure_name}", fg="green"))
                else:
                    click.echo(click.style(
                        f"    ✗ {vr.case.measure_name}: "
                        f"got {vr.metric_view_value}, expected {vr.expected_value} "
                        f"({vr.error or ''})",
                        fg="red",
                    ))
                    all_passed = False
        except Exception as e:
            click.echo(click.style(f"  VALIDATION ERROR: {e}", fg="red"))
            all_passed = False

    if cleanup:
        click.echo("\nCleaning up test views...")
        cleanup_test_views(client, catalog, schema)

    click.echo(f"\n{'═' * 60}")
    if all_passed:
        click.echo(click.style("All tests passed!", fg="green", bold=True))
    else:
        click.echo(click.style("Some tests failed.", fg="red", bold=True))
        sys.exit(1)


if __name__ == "__main__":
    cli()
