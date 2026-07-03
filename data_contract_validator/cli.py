import os
import sys
import json
import yaml
import subprocess
import click
import requests
from pathlib import Path
from typing import Optional, Dict, Any

from .core.validator import ContractValidator
from .extractors.dbt import DBTExtractor
from .extractors.fastapi import FastAPIExtractor


def _github_path_exists(repo: str, path: str, token: Optional[str] = None) -> Optional[bool]:
    """Check whether `path` exists in `repo` via the GitHub contents API.

    Returns True/False when the check is conclusive, or None if it couldn't
    be verified (network error, rate limit, etc.) -- callers should treat
    None as "unknown", not "missing".
    """
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}"} if token else {}
    try:
        response = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException:
        return None

    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    return None


def _github_auth_hint(exists: Optional[bool], token: Optional[str]) -> Optional[str]:
    """Hint to print when a path lookup came back missing and no token was used.

    GitHub's contents API 404s for private repos when unauthenticated, so a
    missing path is ambiguous between "wrong path" and "needs GITHUB_TOKEN".
    """
    if exists is False and not token:
        return "if this is a private repo, set GITHUB_TOKEN first: export GITHUB_TOKEN=$(gh auth token)"
    return None


@click.group()
@click.version_option()
def cli():
    """🛡️ Data Contract Validator - Prevent production API breaks with lightweight governance."""
    pass


@cli.command()
@click.option(
    "--interactive", is_flag=True, help="Interactive setup wizard (recommended)"
)
@click.option(
    "--framework",
    type=click.Choice(["fastapi", "django", "flask"]),
    help="Target framework",
)
@click.option("--dbt-path", default=".", help="DBT project path")
@click.option("--output-dir", default=".", help="Output directory")
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite an existing .retl-validator.yml / workflow file instead of refusing",
)
def init(interactive: bool, framework: str, dbt_path: str, output_dir: str, force: bool):
    """🚀 Initialize contract validation for your project (takes 30 seconds)."""

    click.echo("🛡️ Setting up Data Contract Validation...")
    click.echo("   This prevents production breaks forever!")
    click.echo()

    output_path = Path(output_dir)
    config_file = output_path / ".retl-validator.yml"

    # Re-running init is how people pick up a newer version's config
    # defaults, but .retl-validator.yml commonly accumulates hand-edits
    # (path fixes, mapping.tables entries) -- silently clobbering that on
    # every run would lose real work, so require an explicit --force.
    if config_file.exists() and not force:
        click.echo(f"⚠️  {config_file} already exists — refusing to overwrite it.")
        click.echo("   💡 Re-run with --force to regenerate it from scratch,")
        click.echo("      or edit the existing file directly.")
        sys.exit(1)

    if interactive:
        config = _interactive_setup()
    else:
        config = _quick_setup(framework, dbt_path)

    # Write config file
    with open(config_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)

    click.echo(f"✅ Created configuration: {config_file}")

    # Create GitHub Actions workflow
    if _create_github_workflow(output_path, config, force=force):
        click.echo("✅ Created GitHub Actions workflow")

    # Pre-commit hook is opt-in and asked here so it doesn't require a
    # separate `setup-precommit` invocation for people who want it from the
    # start. Only asked in --interactive mode -- the non-interactive path is
    # meant to run unattended (e.g. scripted/CI setup).
    if interactive:
        click.echo()
        if click.confirm(
            "🪝 Also set up a pre-commit hook? (runs validation on every commit)",
            default=True,
        ):
            install_hooks = click.confirm(
                "   Install it now too (runs `pre-commit install`)?", default=True
            )
            _setup_precommit(install_hooks)

    # Test the setup
    click.echo("\n🧪 Testing your setup...")
    if _test_setup(config_file):
        click.echo("\n🎉 Setup complete! Your contracts are now protected.")
        click.echo("\n🚀 Next steps:")
        add_targets = ".retl-validator.yml .github/workflows/"
        if (output_path / ".pre-commit-config.yaml").exists():
            add_targets += " .pre-commit-config.yaml"
        click.echo(f"   1. git add {add_targets}")
        click.echo("   2. git commit -m 'Add data contract validation'")
        click.echo("   3. git push (triggers validation in CI/CD)")
        click.echo("   4. Watch it prevent production breaks! 🛡️")
    else:
        click.echo(
            "\n⚠️  Setup needs attention. Run 'contract-validator test' for details."
        )


def _interactive_setup() -> Dict[str, Any]:
    """Interactive setup wizard with directory support."""
    click.echo("📋 Quick Setup (a handful of questions):")
    click.echo()

    # Question 1: DBT project location
    dbt_path = click.prompt(
        "1️⃣  Where is your DBT project?", default=".", show_default=True
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
        show_default=True,
    )

    if framework == "fastapi":
        default_path = "app/models"  # Default to directory
        location_noun = "Pydantic models"
    elif framework == "django":
        default_path = "models.py"
        location_noun = "Django models"
    else:
        default_path = "models"
        location_noun = "API models"

    # Question 3: local vs. GitHub -- asked explicitly rather than guessed
    # from the path's shape. A local relative path like "app/models" (the
    # default above) is syntactically identical to a GitHub "org/repo"
    # string, so there's no reliable way to infer this from the string
    # alone; asking up front avoids the wizard silently misreading its own
    # suggested default as a repo.
    click.echo()
    location_kind = click.prompt(
        f"3️⃣  Are your {location_noun} in this local project, or a different GitHub repo?",
        type=click.Choice(["local", "github"]),
        default="local",
        show_default=True,
    )

    click.echo()
    if location_kind == "github":
        repo = click.prompt(
            "4️⃣  GitHub repo (org/repo)", default="", show_default=False
        )
        while "/" not in repo or repo.count("/") != 1:
            click.echo("   ⚠️  Expected the form 'org/repo', e.g. 'my-org/my-api'")
            repo = click.prompt("4️⃣  GitHub repo (org/repo)")

        path = click.prompt(
            f"   Path to your {location_noun} within {repo} (file or directory)",
            default=default_path,
            show_default=True,
        )

        api_config = {"type": "github", "repo": repo, "path": path}

        token = os.environ.get("GITHUB_TOKEN")
        exists = _github_path_exists(repo, path, token)
        if exists is True:
            click.echo(f"   ✅ Path confirmed on GitHub: {path}")
        elif exists is False:
            click.echo(f"   ⚠️  Path not found in {repo}: {path}")
            hint = _github_auth_hint(exists, token)
            if hint:
                click.echo(f"      💡 {hint}")
            if not click.confirm("   Continue anyway?"):
                sys.exit(1)
        # exists is None -> couldn't verify (network issue); stay silent rather
        # than block setup on something that isn't actually wrong.
    else:
        api_location = click.prompt(
            f"4️⃣  Where are your {location_noun}? (file or directory)",
            default=default_path,
            show_default=True,
        )
        api_config = {"type": "local", "path": api_location}

        # Check if local file/directory exists and provide feedback
        local_path = Path(api_location)
        if local_path.exists():
            if local_path.is_file():
                click.echo(f"   ✅ Local file found: {api_location}")
            elif local_path.is_dir():
                # Count Python files in directory
                py_files = list(local_path.rglob("*.py"))
                py_files = [
                    f
                    for f in py_files
                    if not f.name.startswith("test_") and f.name != "__init__.py"
                ]
                click.echo(
                    f"   ✅ Local directory found: {api_location} ({len(py_files)} Python files)"
                )
            else:
                click.echo(
                    f"   ⚠️  Path exists but is neither file nor directory: {api_location}"
                )
        else:
            click.echo(f"   ⚠️  Path not found: {api_location}")
            if not click.confirm("   Continue anyway?"):
                sys.exit(1)

    # Question 5: manifest parsing
    click.echo()
    disable_manifest = click.confirm(
        "5️⃣  Disable manifest.json parsing? (recommended if you have CTE-based models)",
        default=True,
    )

    if disable_manifest:
        click.echo("   📄 Will use SQL file parsing (better for complex models)")
    else:
        click.echo("   📋 Will try manifest.json first, fallback to SQL parsing")

    return {
        "version": "1.0",
        "name": f"contracts-{Path.cwd().name}",
        "source": {
            "dbt": {
                "project_path": dbt_path,
                "auto_compile": True,
                "disable_manifest": disable_manifest,  # NEW
            }
        },
        "target": {framework: api_config},
        "validation": {
            "fail_on": ["missing_tables", "missing_required_columns"],
            "warn_on": ["type_mismatches"],
        },
    }


def _quick_setup(framework: str, dbt_path: str) -> Dict[str, Any]:
    """Quick non-interactive setup with smart defaults."""

    click.echo("🔍 Auto-detecting project structure...")

    # Auto-detect API models location
    framework = framework or "fastapi"

    common_paths = {
        "fastapi": ["app/models.py", "src/models.py", "models.py", "api/models.py"],
        "django": ["models.py", "*/models.py", "app/models.py"],
        "flask": ["models.py", "app/models.py", "src/models.py"],
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
        "source": {"dbt": {"project_path": dbt_path, "auto_compile": True}},
        "target": {framework: {"type": "local", "path": api_path}},
        "validation": {
            "fail_on": ["missing_tables", "missing_required_columns"],
            "warn_on": ["type_mismatches"],
            "mode": "balanced",  # Less strict than interactive
        },
    }


def _create_github_workflow(
    output_path: Path, config: Dict[str, Any], force: bool = False
) -> bool:
    """Auto-create GitHub Actions workflow."""

    workflow_dir = output_path / ".github" / "workflows"
    workflow_file = workflow_dir / "validate-contracts.yml"

    if workflow_file.exists() and not force:
        click.echo(f"   ⚠️  {workflow_file} already exists — leaving it as-is.")
        click.echo("      Use --force to regenerate it.")
        return False

    workflow_dir.mkdir(parents=True, exist_ok=True)

    # Determine trigger paths based on config
    dbt_path = config.get("source", {}).get("dbt", {}).get("project_path", ".")

    trigger_paths = [
        f"{dbt_path}/models/**/*.sql" if dbt_path != "." else "models/**/*.sql",
        f"{dbt_path}/dbt_project.yml" if dbt_path != "." else "dbt_project.yml",
        "**/*models*.py",
        ".retl-validator.yml",
    ]

    # A GitHub target needs a token to fetch the target repo's contents; a
    # local target doesn't talk to the GitHub API at all, so skip the env
    # block entirely rather than wiring an unused token.
    target_info = next(iter(config.get("target", {}).values()), {})
    target_is_github = target_info.get("type") == "github"

    if target_is_github:
        validate_step = """    - name: Validate contracts
      env:
        # secrets.GITHUB_TOKEN is auto-provided by Actions, but it only has
        # access to THIS repository. If the target repo above is private,
        # replace this with a personal access token (repo read access to
        # that specific repo) stored as its own secret, e.g.:
        #   GITHUB_TOKEN: ${{ secrets.API_REPO_TOKEN }}
        # Settings -> Secrets and variables -> Actions -> New repository secret.
        # A public target repo works fine with the default token as-is.
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      run: |
        contract-validator validate \\
          --config .retl-validator.yml \\
          --output github"""
    else:
        validate_step = """    - name: Validate contracts
      run: |
        contract-validator validate \\
          --config .retl-validator.yml \\
          --output github"""

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

{validate_step}
    
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
            🤖 Automated by [Data Contract Validator](https://github.com/OGsiji/data-contract-validator)`
          }})
"""

    try:
        with open(workflow_file, "w") as f:
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

                token = os.environ.get("GITHUB_TOKEN")
                exists = _github_path_exists(repo, path, token)
                if exists is True:
                    click.echo(f"      ✅ Path confirmed: {path}")
                elif exists is False:
                    click.echo(f"      ❌ Path not found: {path}")
                    hint = _github_auth_hint(exists, token)
                    if hint:
                        click.echo(f"         💡 {hint}")
                    all_passed = False
                else:
                    click.echo(
                        "      ⚠️  Could not verify path (network error or rate limit)"
                    )

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
                        target_extractor = FastAPIExtractor.from_local_file(
                            target_info.get("path")
                        )
                        target_schemas = target_extractor.extract_schemas()
                        if target_schemas:
                            click.echo(
                                f"      ✅ Found {len(target_schemas)} API models"
                            )
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
@click.option(
    "--dry-run", is_flag=True, help="Test configuration without full validation"
)
@click.option(
    "--output", type=click.Choice(["terminal", "json", "github"]), default="terminal"
)
@click.option("--dbt-project", help="Override DBT project path")
@click.option(
    "--fastapi-local", help="Override FastAPI models path (file or directory)"
)
@click.option("--fastapi-repo", help="Override FastAPI repo (org/repo)")
@click.option(
    "--fastapi-path",
    default="app/models",
    help="Path in FastAPI repo (file or directory)",
)
@click.option(
    "--disable-manifest", is_flag=True, help="Force SQL parsing, ignore manifest.json"
)
def validate(
    config: str,
    dry_run: bool,
    output: str,
    dbt_project: str,
    fastapi_local: str,
    fastapi_repo: str,
    fastapi_path: str,
    disable_manifest: bool,
):
    """🔍 Validate data contracts (prevents production breaks)."""

    # Load config if it exists
    config_data = {}
    config_file = Path(config)
    if config_file.exists():
        try:
            with open(config_file) as f:
                config_data = yaml.safe_load(f)

            # Validate that config is a dictionary
            if not isinstance(config_data, dict):
                click.echo(
                    f"❌ Configuration file {config} is invalid: expected YAML dictionary"
                )
                click.echo(
                    "💡 Check the file format - it should contain key-value pairs"
                )
                sys.exit(1)

            # Check for required sections
            if "source" not in config_data:
                click.echo(f"⚠️  Warning: 'source' section missing in {config}")
            if "target" not in config_data:
                click.echo(f"⚠️  Warning: 'target' section missing in {config}")

            click.echo(f"📋 Using config: {config}")
        except yaml.YAMLError as e:
            click.echo(f"❌ Configuration file {config} contains invalid YAML:")
            click.echo(f"   {e}")
            click.echo("💡 Check for:")
            click.echo("   - Incorrect indentation")
            click.echo("   - Missing colons after keys")
            click.echo("   - Unmatched quotes or brackets")
            sys.exit(1)
        except Exception as e:
            click.echo(f"❌ Error reading configuration file {config}: {e}")
            sys.exit(1)
    elif not any([dbt_project, fastapi_local, fastapi_repo]):
        click.echo("❌ No config file found and no command line options provided")
        click.echo("💡 Run 'contract-validator init' to create a config file")
        click.echo("   Or use command line options:")
        click.echo(
            "   contract-validator validate --dbt-project . --fastapi-local app/models.py"
        )
        sys.exit(1)

    if dry_run:
        click.echo("🧪 Dry run - testing configuration only")
        _test_configuration(
            config_data, dbt_project, fastapi_local, fastapi_repo, disable_manifest
        )
        return

    # Run actual validation
    _run_validation(
        config_data,
        output,
        dbt_project,
        fastapi_local,
        fastapi_repo,
        fastapi_path,
        disable_manifest,
    )


def _test_configuration(
    config_data: Dict[str, Any],
    dbt_project: str,
    fastapi_local: str,
    fastapi_repo: str,
    disable_manifest: bool = False,
):
    """Test configuration without running full validation."""

    dbt_path = dbt_project or config_data.get("source", {}).get("dbt", {}).get(
        "project_path", "."
    )

    click.echo(f"   📊 DBT project: {dbt_path}")
    if Path(dbt_path).exists():
        click.echo("      ✅ Path exists")
    else:
        click.echo("      ❌ Path not found")

    if disable_manifest or config_data.get("source", {}).get("dbt", {}).get(
        "disable_manifest", False
    ):
        click.echo("   📄 Manifest parsing: disabled")
    else:
        click.echo("   📋 Manifest parsing: enabled")

    if fastapi_local:
        click.echo(f"   🎯 FastAPI models: {fastapi_local}")
        local_path = Path(fastapi_local)
        if local_path.exists():
            if local_path.is_file():
                click.echo("      ✅ File exists")
            elif local_path.is_dir():
                click.echo("      ✅ Directory exists")
        else:
            click.echo("      ❌ Path not found")

    if fastapi_repo:
        click.echo(f"   🐙 FastAPI repo: {fastapi_repo}")

    click.echo("✅ Configuration test complete!")


def _run_validation(
    config_data: Dict[str, Any],
    output: str,
    dbt_project: str,
    fastapi_local: str,
    fastapi_repo: str,
    fastapi_path: str,
    disable_manifest: bool = False,
):
    """Run the actual validation with manifest disable option."""

    # Get DBT project path
    dbt_path = dbt_project or config_data.get("source", {}).get("dbt", {}).get(
        "project_path", "."
    )

    # Get disable_manifest from config file OR command line flag
    config_disable_manifest = (
        config_data.get("source", {}).get("dbt", {}).get("disable_manifest", False)
    )
    use_disable_manifest = (
        disable_manifest or config_disable_manifest
    )  # CLI flag takes precedence

    if use_disable_manifest:
        click.echo("📄 Manifest parsing disabled")
        if disable_manifest:
            click.echo("   (via --disable-manifest flag)")
        else:
            click.echo("   (via .retl-validator.yml config)")

    # Initialize DBT extractor with disable_manifest option
    try:
        dbt_extractor = DBTExtractor(dbt_path, disable_manifest=use_disable_manifest)
    except Exception as e:
        click.echo(f"❌ Error initializing DBT extractor: {e}")
        sys.exit(1)

    # Initialize FastAPI extractor with directory support
    try:
        if fastapi_local:
            # Use local path (file or directory)
            local_path = fastapi_local

            # Auto-detect if it's a file or directory
            path = Path(local_path)
            if path.is_file():
                click.echo(f"📄 Using FastAPI models file: {local_path}")
                fastapi_extractor = FastAPIExtractor.from_local_file(local_path)
            elif path.is_dir():
                click.echo(f"📁 Using FastAPI models directory: {local_path}")
                fastapi_extractor = FastAPIExtractor.from_local_directory(local_path)
            else:
                raise ValueError(f"Path does not exist: {local_path}")

        elif fastapi_repo:
            # Use GitHub repository
            github_token = os.environ.get("GITHUB_TOKEN")

            # Check if fastapi_path ends with .py (file) or not (directory)
            if fastapi_path.endswith(".py"):
                click.echo(
                    f"📄 Using FastAPI models file: {fastapi_repo}/{fastapi_path}"
                )
            else:
                click.echo(
                    f"📁 Using FastAPI models directory: {fastapi_repo}/{fastapi_path}"
                )

            fastapi_extractor = FastAPIExtractor.from_github_repo(
                repo=fastapi_repo, path=fastapi_path, token=github_token
            )
        else:
            # Get from config
            target_config = list(config_data.get("target", {}).values())[0]
            if target_config.get("type") == "local":
                local_path = target_config.get("path")
                path = Path(local_path)

                if path.is_file():
                    fastapi_extractor = FastAPIExtractor.from_local_file(local_path)
                elif path.is_dir():
                    fastapi_extractor = FastAPIExtractor.from_local_directory(
                        local_path
                    )
                else:
                    raise ValueError(f"Path does not exist: {local_path}")

            elif target_config.get("type") == "github":
                github_token = os.environ.get("GITHUB_TOKEN")
                fastapi_extractor = FastAPIExtractor.from_github_repo(
                    repo=target_config.get("repo"),
                    path=target_config.get("path", "app/models"),
                    token=github_token,
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
            target_extractor=fastapi_extractor,
            mapping=config_data.get("mapping"),
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
        fail_on = validation_config.get(
            "fail_on", ["missing_tables", "missing_required_columns"]
        )

        if "missing_tables" in fail_on and any(
            "Missing Table" in issue.category for issue in result.critical_issues
        ):
            sys.exit(1)
        elif "missing_required_columns" in fail_on and any(
            "Missing Column" in issue.category for issue in result.critical_issues
        ):
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


@cli.command()
@click.option("--install-hooks", is_flag=True, help="Install pre-commit hooks")
def setup_precommit(install_hooks: bool):
    """🛠️ Setup pre-commit integration for contract validation."""
    _setup_precommit(install_hooks)


def _setup_precommit(install_hooks: bool) -> None:
    """Write .pre-commit-config.yaml (if absent) and optionally install the hook."""

    click.echo("🛠️ Setting up pre-commit integration...")

    # Create .pre-commit-config.yaml if it doesn't exist
    precommit_config = Path(".pre-commit-config.yaml")

    if not precommit_config.exists():
        # Generate correct pre-commit config with proper indentation
        config_content = """repos:
  - repo: https://github.com/OGsiji/data-contract-validator
    rev: main
    hooks:
      - id: contract-validation
        name: Validate Data Contracts
        files: '^(.*models.*\\.(sql|py)|\\.retl-validator\\.yml|dbt_project\\.yml)$'

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
"""

        with open(precommit_config, "w") as f:
            f.write(config_content)

        click.echo(f"✅ Created {precommit_config}")
    else:
        click.echo(f"⚠️  {precommit_config} already exists")

    if install_hooks:
        # Install pre-commit if not installed
        try:
            subprocess.run(["pre-commit", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            click.echo("📦 Installing pre-commit...")
            subprocess.run([sys.executable, "-m", "pip", "install", "pre-commit"])

        # Install hooks
        click.echo("🔗 Installing pre-commit hooks...")
        result = subprocess.run(
            ["pre-commit", "install"], capture_output=True, text=True
        )

        if result.returncode == 0:
            click.echo("✅ Pre-commit hooks installed successfully!")
            click.echo(
                "\n🎉 Setup complete! Now contract validation runs on every commit."
            )
            click.echo("\n🧪 Test it:")
            click.echo("   git add .")
            click.echo("   git commit -m 'test contract validation'")
            click.echo("   # Contract validation will run automatically!")
        else:
            click.echo(f"❌ Failed to install hooks: {result.stderr}")
    else:
        click.echo("\n🔗 To complete setup, run:")
        click.echo("   pip install pre-commit")
        click.echo("   pre-commit install")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
