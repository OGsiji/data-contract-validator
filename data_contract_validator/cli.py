"""
Command line interface for the data contract validator.
"""

import sys
import json
import click
from pathlib import Path
from typing import Optional

from .core.validator import ContractValidator
from .extractors.dbt import DBTExtractor  
from .extractors.fastapi import FastAPIExtractor


@click.group()
@click.version_option()
def cli():
    """Data Contract Validator - Prevent production API breaks."""
    pass


@cli.command()
@click.option("--dbt-project", default=".", help="Path to DBT project")
@click.option("--fastapi-repo", help="GitHub repository (org/repo)")
@click.option("--fastapi-local", help="Local path to FastAPI models")
@click.option("--fastapi-path", default="app/models.py", help="Path to models in repo")
@click.option("--github-token", help="GitHub token for private repos")
@click.option("--output", type=click.Choice(["terminal", "json", "github"]), default="terminal")
@click.option("--fail-on", type=click.Choice(["critical", "warning"]), default="critical")
def validate(dbt_project: str, fastapi_repo: Optional[str], fastapi_local: Optional[str], 
             fastapi_path: str, github_token: Optional[str], output: str, fail_on: str):
    """Validate data contracts between DBT and FastAPI."""
    
    # Initialize DBT extractor
    dbt_extractor = DBTExtractor(dbt_project)
    
    # Initialize FastAPI extractor
    if fastapi_local:
        fastapi_extractor = FastAPIExtractor.from_local_file(fastapi_local)
    elif fastapi_repo:
        fastapi_extractor = FastAPIExtractor.from_github_repo(
            repo=fastapi_repo,
            path=fastapi_path,
            token=github_token
        )
    else:
        click.echo("❌ Must specify either --fastapi-repo or --fastapi-local", err=True)
        sys.exit(1)
    
    # Run validation
    validator = ContractValidator(
        source_extractor=dbt_extractor,
        target_extractor=fastapi_extractor
    )
    
    result = validator.validate()
    
    # Output results
    if output == "json":
        click.echo(json.dumps(result.to_dict(), indent=2))
    elif output == "github":
        _output_github_actions(result)
    else:
        _output_terminal(result)
    
    # Exit with appropriate code
    if fail_on == "critical" and result.critical_issues:
        sys.exit(1)
    elif fail_on == "warning" and result.issues:
        sys.exit(1)


@cli.command()
@click.option("--framework", type=click.Choice(["fastapi", "django"]), default="fastapi")
@click.option("--output-dir", default=".", help="Output directory")
def init(framework: str, output_dir: str):
    """Initialize configuration files for contract validation."""
    
    output_path = Path(output_dir)
    
    # Create configuration file
    config_file = output_path / ".contract-validator.yml"
    
    config_content = f"""version: '1.0'
sources:
  dbt:
    project_path: './dbt-project'
    auto_update_schemas: true

targets:
  {framework}:
    repo: 'my-org/my-api'  # Update this
    path: 'app/models.py'   # Update this
    
validation:
  fail_on: ['missing_tables', 'missing_required_columns']
  warn_on: ['type_mismatches', 'missing_optional_columns']
"""
    
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    click.echo(f"✅ Created configuration file: {config_file}")
    click.echo("✏️  Edit the file to match your project structure")


def _output_terminal(result):
    """Output results to terminal."""
    click.echo(f"\n📊 Validation Summary:")
    click.echo(f"   Success: {result.success}")
    click.echo(f"   Total issues: {len(result.issues)}")
    click.echo(f"   Critical: {len(result.critical_issues)}")
    click.echo(f"   Warnings: {len(result.warnings)}")
    
    if result.critical_issues:
        click.echo("\n🚨 Critical Issues:")
        for issue in result.critical_issues:
            click.echo(f"  💥 {issue.table}.{issue.column}: {issue.message}")
            if issue.suggested_fix:
                click.echo(f"     🔧 Fix: {issue.suggested_fix}")
    
    if result.warnings and not result.critical_issues:
        click.echo("\n⚠️  Warnings:")
        for issue in result.warnings[:5]:
            click.echo(f"  ⚠️  {issue.table}.{issue.column}: {issue.message}")
        
        if len(result.warnings) > 5:
            click.echo(f"  ... and {len(result.warnings) - 5} more warnings")
    
    click.echo(f"\n{result.summary}")


def _output_github_actions(result):
    """Output results for GitHub Actions."""
    if result.success:
        click.echo("✅ Contract validation passed")
        click.echo(f"::notice::Validation successful - {result.summary}")
    else:
        click.echo("❌ Contract validation failed")
        click.echo(f"::error::Validation failed - {result.summary}")
        
        for issue in result.critical_issues:
            click.echo(f"::error::{issue.table}.{issue.column}: {issue.message}")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()