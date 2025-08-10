import os
import sys
import json
import yaml
import subprocess
import click
from pathlib import Path
from typing import Optional, Dict, Any

from .core.validator import ContractValidator
from .extractors.dbt import DBTExtractor  
from .extractors.fastapi import FastAPIExtractor


@click.group()
@click.version_option()
def cli():
    """🛡️ Data Contract Validator - Prevent production API breaks with lightweight governance."""
    pass


@cli.command()
@click.option("--interactive", is_flag=True, help="Interactive setup wizard (recommended)")
@click.option("--framework", type=click.Choice(["fastapi", "django", "flask"]), help="Target framework")
@click.option("--dbt-path", default=".", help="DBT project path")
@click.option("--output-dir", default=".", help="Output directory")
def init(interactive: bool, framework: str, dbt_path: str, output_dir: str):
    """🚀 Initialize contract validation for your project (takes 30 seconds)."""
    
    click.echo("🛡️ Setting up Data Contract Validation...")
    click.echo("   This prevents production breaks forever!")
    click.echo()
    
    if interactive:
        config = _interactive_setup()
    else:
        config = _quick_setup(framework, dbt_path)
    
    output_path = Path(output_dir)
    
    # Write config file
    config_file = output_path / ".retl-validator.yml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)
    
    click.echo(f"✅ Created configuration: {config_file}")
    
    # Create GitHub Actions workflow
    if _create_github_workflow(output_path, config):
        click.echo("✅ Created GitHub Actions workflow")
    
    # Test the setup
    click.echo("\n🧪 Testing your setup...")
    if _test_setup(config_file):
        click.echo("\n🎉 Setup complete! Your contracts are now protected.")
        click.echo("\n🚀 Next steps:")
        click.echo("   1. git add .retl-validator.yml .github/workflows/")
        click.echo("   2. git commit -m 'Add data contract validation'")
        click.echo("   3. git push (triggers validation in CI/CD)")
        click.echo("   4. Watch it prevent production breaks! 🛡️")
    else:
        click.echo("\n⚠️  Setup needs attention. Run 'contract-validator test' for details.")


def _interactive_setup() -> Dict[str, Any]:
    """Interactive setup wizard - 3 simple questions."""
    click.echo("📋 Quick Setup (3 questions):")
    click.echo()
    
    # Question 1: DBT project location
    dbt_path = click.prompt(
        "1️⃣  Where is your DBT project?", 
        default=".",
        show_default=True
    )
    
    # Auto-detect if DBT project exists
    if not Path(dbt_path).exists() or not (Path(dbt_path) / "dbt_project.yml").exists():
        click.echo(f"   ⚠️  No dbt_project.yml found at {dbt_path}")
        if click.confirm("   Continue anyway?"):
            pass
        else:
            click.echo("   💡 Make sure you're in your DBT project directory")
            sys.exit(1)
    else:
        click.echo("   ✅ DBT project found")
    
    # Question 2: API framework
    click.echo()
    framework = click.prompt(
        "2️⃣  What API framework do you use?", 
        type=click.Choice(["fastapi", "django", "flask", "other"]),
        default="fastapi",
        show_default=True
    )
    
    # Question 3: API models location
    click.echo()
    if framework == "fastapi":
        default_path = "app/models.py"
        prompt_text = "3️⃣  Where are your Pydantic models?"
    elif framework == "django":
        default_path = "models.py"
        prompt_text = "3️⃣  Where are your Django models?"
    else:
        default_path = "models.py"
        prompt_text = "3️⃣  Where are your API models?"
    
    api_location = click.prompt(
        prompt_text,
        default=default_path,
        show_default=True
    )
    
    # Auto-detect if it's local file or GitHub repo
    is_github_repo = "/" in api_location and not api_location.startswith((".", "/"))
    
    if is_github_repo:
        # Format: "org/repo" or "org/repo/path/to/file.py"
        parts = api_location.split("/")
        if len(parts) >= 2:
            repo = "/".join(parts[:2])
            path = "/".join(parts[2:]) if len(parts) > 2 else "models.py"
        else:
            repo = api_location
            path = "models.py"
            
        api_config = {
            "type": "github",
            "repo": repo,
            "path": path
        }
        click.echo(f"   🐙 GitHub repo detected: {repo}/{path}")
    else:
        api_config = {
            "type": "local", 
            "path": api_location
        }
        
        # Check if local file exists
        if Path(api_location).exists():
            click.echo("   ✅ Local file found")
        else:
            click.echo(f"   ⚠️  File not found: {api_location}")
            if not click.confirm("   Continue anyway?"):
                sys.exit(1)
    
    return {
        "version": "1.0",
        "name": f"contracts-{Path.cwd().name}",
        "description": "Auto-generated data contract validation",
        "source": {
            "dbt": {
                "project_path": dbt_path,
                "auto_compile": True,
                "timeout": 120
            }
        },
        "target": {
            framework: api_config
        },
        "validation": {
            "fail_on": ["missing_tables", "missing_required_columns"],
            "warn_on": ["type_mismatches", "missing_optional_columns"],
            "mode": "strict"
        },
        "output": {
            "format": "terminal",
            "show_suggestions": True,
            "max_issues": 20
        }
    }


def _quick_setup(framework: str, dbt_path: str) -> Dict[str, Any]:
    """Quick non-interactive setup with smart defaults."""
    
    click.echo("🔍 Auto-detecting project structure...")
    
    # Auto-detect API models location
    framework = framework or "fastapi"
    
    common_paths = {
        "fastapi": ["app/models.py", "src/models.py", "models.py", "api/models.py"],
        "django": ["models.py", "*/models.py", "app/models.py"],
        "flask": ["models.py", "app/models.py", "src/models.py"]
    }
    
    api_path = None
    if framework in common_paths:
        for path in common_paths[framework]:
            if "*" in path:
                # Handle wildcard patterns
                import glob
                matches = glob.glob(path)
                if matches:
                    api_path = matches[0]
                    break
            elif Path(path).exists():
                api_path = path
                break
    
    if api_path:
        click.echo(f"   ✅ Found {framework} models: {api_path}")
    else:
        api_path = common_paths[framework][0]  # Use default
        click.echo(f"   ⚠️  Using default path: {api_path}")
    
    return {
        "version": "1.0",
        "name": f"contracts-{Path.cwd().name}",
        "description": "Auto-generated data contract validation",
        "source": {
            "dbt": {
                "project_path": dbt_path,
                "auto_compile": True
            }
        },
        "target": {
            framework: {
                "type": "local",
                "path": api_path
            }
        },
        "validation": {
            "fail_on": ["missing_tables", "missing_required_columns"],
            "warn_on": ["type_mismatches"],
            "mode": "balanced"  # Less strict than interactive
        }
    }


def _create_github_workflow(output_path: Path, config: Dict[str, Any]) -> bool:
    """Auto-create GitHub Actions workflow."""
    
    workflow_dir = output_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine trigger paths based on config
    dbt_path = config.get("source", {}).get("dbt", {}).get("project_path", ".")
    
    trigger_paths = [
        f"{dbt_path}/models/**/*.sql" if dbt_path != "." else "models/**/*.sql",
        f"{dbt_path}/dbt_project.yml" if dbt_path != "." else "dbt_project.yml",
        "**/*models*.py",
        ".retl-validator.yml"
    ]
    
    workflow_content = f"""# 🤖 Auto-generated by data-contract-validator
name: 🛡️ Data Contract Validation

on:
  pull_request:
    paths:
{chr(10).join(f'      - "{path}"' for path in trigger_paths)}
  
  workflow_dispatch:

permissions:
  contents: read
  pull-requests: write

jobs:
  validate-contracts:
    name: Validate Data Contracts
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install data contract validator
      run: pip install data-contract-validator
    
    - name: Validate contracts
      env:
        GITHUB_TOKEN: ${{{{ secrets.GITHUB_TOKEN }}}}
      run: |
        contract-validator validate \\
          --config .retl-validator.yml \\
          --output github
    
    - name: Comment on PR (if validation fails)
      if: failure()
      uses: actions/github-script@v6
      with:
        script: |
          github.rest.issues.createComment({{
            issue_number: context.issue.number,
            owner: context.repo.owner,
            repo: context.repo.repo,
            body: `## 🚨 Data Contract Validation Failed
            
            Your changes don't satisfy API requirements.
            Check the logs above for specific issues.
            
            **Common fixes:**
            - Add missing columns to your DBT model
            - Update API models to match DBT output
            - Check for type mismatches
            
            ---
            🤖 Automated by [Data Contract Validator](https://github.com/OGsiji/retl_validator)`
          }})
"""
    
    workflow_file = workflow_dir / "validate-contracts.yml"
    try:
        with open(workflow_file, 'w') as f:
            f.write(workflow_content)
        return True
    except Exception as e:
        click.echo(f"   ⚠️  Could not create workflow: {e}")
        return False


@cli.command()
def test():
    """🧪 Test your contract validation setup."""
    
    click.echo("🧪 Testing Data Contract Validation Setup...")
    click.echo("=" * 45)
    
    config_file = Path(".retl-validator.yml")
    return _test_setup(config_file)


def _test_setup(config_file: Path) -> bool:
    """Internal setup test with detailed output."""
    
    all_passed = True
    
    # Test 1: Config file exists
    click.echo("\n1️⃣  Checking configuration file...")
    if not config_file.exists():
        click.echo(f"   ❌ No {config_file} found")
        click.echo("   💡 Run 'contract-validator init' first")
        return False
    
    click.echo(f"   ✅ Configuration file found: {config_file}")
    
    # Test 2: Load and validate config
    click.echo("\n2️⃣  Validating configuration...")
    try:
        with open(config_file) as f:
            config = yaml.safe_load(f)
        click.echo("   ✅ Configuration is valid YAML")
        
        # Check required sections
        required_sections = ["version", "source", "target", "validation"]
        missing_sections = [s for s in required_sections if s not in config]
        if missing_sections:
            click.echo(f"   ⚠️  Missing sections: {missing_sections}")
            all_passed = False
        else:
            click.echo("   ✅ All required sections present")
            
    except Exception as e:
        click.echo(f"   ❌ Configuration file is invalid: {e}")
        return False
    
    # Test 3: Check DBT project
    click.echo("\n3️⃣  Checking DBT project...")
    dbt_config = config.get("source", {}).get("dbt", {})
    dbt_path = Path(dbt_config.get("project_path", "."))
    
    if not dbt_path.exists():
        click.echo(f"   ❌ DBT project directory not found: {dbt_path}")
        all_passed = False
    elif not (dbt_path / "dbt_project.yml").exists():
        click.echo(f"   ⚠️  No dbt_project.yml found in {dbt_path}")
        click.echo("   💡 Make sure this is a valid DBT project")
        all_passed = False
    else:
        click.echo(f"   ✅ DBT project found: {dbt_path}")
    
    # Test 4: Check target configuration
    click.echo("\n4️⃣  Checking target configuration...")
    target_config = config.get("target", {})
    
    if not target_config:
        click.echo("   ❌ No target configuration found")
        all_passed = False
    else:
        for target_name, target_info in target_config.items():
            click.echo(f"   🎯 Target: {target_name}")
            
            if target_info.get("type") == "local":
                api_path = Path(target_info.get("path", ""))
                if not api_path.exists():
                    click.echo(f"      ⚠️  Local file not found: {api_path}")
                    all_passed = False
                else:
                    click.echo(f"      ✅ Local file found: {api_path}")
                    
            elif target_info.get("type") == "github":
                repo = target_info.get("repo")
                path = target_info.get("path")
                click.echo(f"      🐙 GitHub repo: {repo}/{path}")
                
            else:
                click.echo(f"      ⚠️  Unknown target type: {target_info.get('type')}")
                all_passed = False
    
    # Test 5: Try a dry run validation
    click.echo("\n5️⃣  Testing validation...")
    try:
        from .core.validator import ContractValidator
        from .extractors.dbt import DBTExtractor
        from .extractors.fastapi import FastAPIExtractor
        
        # Quick validation test
        dbt_extractor = DBTExtractor(str(dbt_path))
        
        # Test DBT extraction
        click.echo("   🔍 Testing DBT extraction...")
        dbt_schemas = dbt_extractor.extract_schemas()
        if dbt_schemas:
            click.echo(f"      ✅ Found {len(dbt_schemas)} DBT models")
        else:
            click.echo("      ⚠️  No DBT models found")
            all_passed = False
        
        # Test target extraction (for local files only)
        for target_name, target_info in target_config.items():
            if target_info.get("type") == "local":
                click.echo(f"   🎯 Testing {target_name} extraction...")
                try:
                    if target_name == "fastapi":
                        target_extractor = FastAPIExtractor.from_local_file(target_info.get("path"))
                        target_schemas = target_extractor.extract_schemas()
                        if target_schemas:
                            click.echo(f"      ✅ Found {len(target_schemas)} API models")
                        else:
                            click.echo("      ⚠️  No API models found")
                            all_passed = False
                except Exception as e:
                    click.echo(f"      ⚠️  Extraction error: {e}")
                    all_passed = False
        
    except Exception as e:
        click.echo(f"   ⚠️  Validation test error: {e}")
        all_passed = False
    
    # Final result
    click.echo("\n" + "=" * 45)
    if all_passed:
        click.echo("🎉 All tests passed! Your setup is ready.")
        click.echo("\n🚀 Next steps:")
        click.echo("   • Run 'contract-validator validate' to test validation")
        click.echo("   • Commit your config and workflow files")
        click.echo("   • Push to activate protection in CI/CD")
    else:
        click.echo("⚠️  Some tests had issues. See details above.")
        click.echo("\n💡 Common fixes:")
        click.echo("   • Make sure you're in your DBT project directory")
        click.echo("   • Check that API model files exist")
        click.echo("   • Run 'contract-validator init' to regenerate config")
    
    return all_passed


@cli.command()
@click.option("--config", default=".retl-validator.yml", help="Config file path")
@click.option("--dry-run", is_flag=True, help="Test configuration without full validation")
@click.option("--output", type=click.Choice(["terminal", "json", "github"]), default="terminal")
@click.option("--dbt-project", help="Override DBT project path")
@click.option("--fastapi-local", help="Override FastAPI models path")
@click.option("--fastapi-repo", help="Override FastAPI repo (org/repo)")
@click.option("--fastapi-path", default="app/models.py", help="Path in FastAPI repo")
def validate(config: str, dry_run: bool, output: str, dbt_project: str, fastapi_local: str, fastapi_repo: str, fastapi_path: str):
    """🔍 Validate data contracts (prevents production breaks)."""
    
    # Load config if it exists
    config_data = {}
    config_file = Path(config)
    if config_file.exists():
        with open(config_file) as f:
            config_data = yaml.safe_load(f)
        click.echo(f"📋 Using config: {config}")
    elif not any([dbt_project, fastapi_local, fastapi_repo]):
        click.echo("❌ No config file found and no command line options provided")
        click.echo("💡 Run 'contract-validator init' to create a config file")
        click.echo("   Or use command line options:")
        click.echo("   contract-validator validate --dbt-project . --fastapi-local app/models.py")
        sys.exit(1)
    
    if dry_run:
        click.echo("🧪 Dry run - testing configuration only")
        _test_configuration(config_data, dbt_project, fastapi_local, fastapi_repo)
        return
    
    # Run actual validation
    _run_validation(config_data, output, dbt_project, fastapi_local, fastapi_repo, fastapi_path)


def _test_configuration(config_data: Dict[str, Any], dbt_project: str, fastapi_local: str, fastapi_repo: str):
    """Test configuration without running full validation."""
    
    dbt_path = dbt_project or config_data.get("source", {}).get("dbt", {}).get("project_path", ".")
    
    click.echo(f"   📊 DBT project: {dbt_path}")
    if Path(dbt_path).exists():
        click.echo("      ✅ Path exists")
    else:
        click.echo("      ❌ Path not found")
    
    if fastapi_local:
        click.echo(f"   🎯 FastAPI models: {fastapi_local}")
        if Path(fastapi_local).exists():
            click.echo("      ✅ File exists")
        else:
            click.echo("      ❌ File not found")
    
    if fastapi_repo:
        click.echo(f"   🐙 FastAPI repo: {fastapi_repo}")
    
    click.echo("✅ Configuration test complete!")


def _run_validation(config_data: Dict[str, Any], output: str, dbt_project: str, fastapi_local: str, fastapi_repo: str, fastapi_path: str):
    """Run the actual validation."""
    
    # Get DBT project path
    dbt_path = dbt_project or config_data.get("source", {}).get("dbt", {}).get("project_path", ".")
    
    # Initialize DBT extractor
    try:
        dbt_extractor = DBTExtractor(dbt_path)
    except Exception as e:
        click.echo(f"❌ Error initializing DBT extractor: {e}")
        sys.exit(1)
    
    # Initialize FastAPI extractor
    try:
        if fastapi_local:
            fastapi_extractor = FastAPIExtractor.from_local_file(fastapi_local)
        elif fastapi_repo:
            github_token = os.environ.get("GITHUB_TOKEN")
            fastapi_extractor = FastAPIExtractor.from_github_repo(
                repo=fastapi_repo,
                path=fastapi_path,
                token=github_token
            )
        else:
            # Get from config
            target_config = list(config_data.get("target", {}).values())[0]
            if target_config.get("type") == "local":
                fastapi_extractor = FastAPIExtractor.from_local_file(target_config.get("path"))
            elif target_config.get("type") == "github":
                github_token = os.environ.get("GITHUB_TOKEN")
                fastapi_extractor = FastAPIExtractor.from_github_repo(
                    repo=target_config.get("repo"),
                    path=target_config.get("path", "app/models.py"),
                    token=github_token
                )
            else:
                click.echo("❌ No valid FastAPI configuration found")
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"❌ Error initializing FastAPI extractor: {e}")
        sys.exit(1)
    
    # Run validation
    try:
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
        validation_config = config_data.get("validation", {})
        fail_on = validation_config.get("fail_on", ["missing_tables", "missing_required_columns"])
        
        if "missing_tables" in fail_on and any("Missing Table" in issue.category for issue in result.critical_issues):
            sys.exit(1)
        elif "missing_required_columns" in fail_on and any("Missing Column" in issue.category for issue in result.critical_issues):
            sys.exit(1)
        elif result.critical_issues:
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Validation error: {e}")
        sys.exit(1)


def _output_terminal(result):
    """Output results to terminal with emojis and colors."""
    click.echo(f"\n🛡️ Data Contract Validation Results:")
    click.echo("=" * 45)
    click.echo(f"Status: {'✅ PASSED' if result.success else '❌ FAILED'}")
    click.echo(f"Total issues: {len(result.issues)}")
    click.echo(f"Critical: {len(result.critical_issues)}")
    click.echo(f"Warnings: {len(result.warnings)}")
    
    if result.critical_issues:
        click.echo("\n🚨 Critical Issues (Must Fix):")
        for issue in result.critical_issues:
            click.echo(f"  💥 {issue.table}")
            if issue.column:
                click.echo(f"     Column: {issue.column}")
            click.echo(f"     Problem: {issue.message}")
            if issue.suggested_fix:
                click.echo(f"     🔧 Fix: {issue.suggested_fix}")
            click.echo()
    
    if result.warnings and not result.critical_issues:
        click.echo("\n⚠️  Warnings (Good to Know):")
        for issue in result.warnings[:5]:
            click.echo(f"  ⚠️  {issue.table}.{issue.column}: {issue.message}")
        
        if len(result.warnings) > 5:
            click.echo(f"  ... and {len(result.warnings) - 5} more warnings")
    
    click.echo(f"\n{result.summary}")
    
    if result.success:
        click.echo("\n🎉 Great! Your API contracts are protected.")
    else:
        click.echo("\n💡 Fix the critical issues above to proceed.")


def _output_github_actions(result):
    """Output results for GitHub Actions."""
    if result.success:
        click.echo("✅ Data contract validation passed")
        click.echo(f"::notice::Validation successful - {result.summary}")
    else:
        click.echo("❌ Data contract validation failed")
        click.echo(f"::error::Validation failed - {result.summary}")
        
        for issue in result.critical_issues:
            click.echo(f"::error::{issue.table}.{issue.column}: {issue.message}")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()