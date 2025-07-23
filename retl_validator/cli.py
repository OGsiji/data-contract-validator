#!/usr/bin/env python3
"""
retl_validator/cli.py
Main CLI that integrates your existing code with CI/CD optimizations
"""

import os
import sys
import time
import json
import yaml
import argparse
import tempfile
from pathlib import Path
from typing import Dict, List, Any

# Import the core components we just created
from .core.validator import YAMLContractValidator
from .core.models import ValidationIssue, ValidationSeverity

# CI/CD Integration classes
class ValidationMode:
    FAST = "fast"
    DIFF = "diff"  
    FULL = "full"

class CICDValidator:
    """
    CI/CD Optimized validator that uses your existing extraction logic
    This bridges the gap between your existing code and CI/CD requirements
    """
    
    def __init__(self, config_path: str = '.retl-validator.yml'):
        self.config_path = Path(config_path)
        self.cache_dir = Path('.retl-validator-cache')
        self.cache_dir.mkdir(exist_ok=True)
        
        # Load configuration
        if self.config_path.exists():
            with open(self.config_path) as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self._get_default_config()
    
    def validate_for_cicd(self, mode: str, changed_files: List[str] = None) -> Dict[str, Any]:
        """
        Main entry point that adapts your existing validator for CI/CD
        """
        
        start_time = time.time()
        print(f"🔍 Running contract validation in {mode} mode...")
        
        if mode == ValidationMode.FAST and changed_files:
            # Fast mode: Only validate if relevant files changed
            if not self._has_relevant_changes(changed_files):
                print("   No schema files changed, skipping validation")
                return {
                    'success': True,
                    'duration': time.time() - start_time,
                    'issues': [],
                    'summary': "✅ No relevant changes detected",
                    'mode': mode
                }
        
        # Create temporary YAML files from config
        fastapi_yaml_path, dbt_yaml_path = self._create_temp_yaml_files()
        
        try:
            # Use your existing YAMLContractValidator
            validator = YAMLContractValidator(
                fastapi_yaml_path=fastapi_yaml_path,
                dbt_yaml_path=dbt_yaml_path
            )
            
            # Store the config so your validator can access it for extraction
            validator.config = self.config
            
            success, issues = validator.validate_contracts()
            
            duration = time.time() - start_time
            
            # Format results for CI/CD
            result = {
                'success': success,
                'duration': duration,
                'issues': [issue.to_dict() for issue in issues],
                'summary': self._generate_summary(issues),
                'mode': mode
            }
            
            print(f"✅ Validation completed in {duration:.2f}s")
            return result
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return {
                'success': False,
                'duration': time.time() - start_time,
                'issues': [],
                'summary': f"❌ Error: {e}",
                'mode': mode
            }
        finally:
            # Clean up temp files
            try:
                os.unlink(fastapi_yaml_path)
                os.unlink(dbt_yaml_path)
            except:
                pass

    def _create_temp_yaml_files(self) -> tuple:
        """Create temporary YAML files from config"""
        
        # Create temporary FastAPI YAML
        fastapi_content = {
            'version': '1.0',
            'generated_from': 'cli_config', 
            'tables': {}  # Your existing extractor will populate this
        }
        
        # Create temporary DBT YAML
        dbt_content = {
            'version': '1.0',
            'generated_from': 'cli_config',
            'tables': {}  # Your existing extractor will populate this
        }
        
        # Write to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(fastapi_content, f)
            fastapi_yaml_path = f.name
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(dbt_content, f)
            dbt_yaml_path = f.name
        
        return fastapi_yaml_path, dbt_yaml_path
    
    def _has_relevant_changes(self, changed_files: List[str]) -> bool:
        """Check if any relevant files changed"""
        
        relevant_patterns = [
            'models/',           # DBT models
            'app/models',        # FastAPI models
            'models.py',         # Django models
            '.retl-validator.yml' # Config changes
        ]
        
        relevant_files = [
            f for f in changed_files 
            if any(pattern in f for pattern in relevant_patterns)
            and (f.endswith('.sql') or f.endswith('.py') or f.endswith('.yml'))
        ]
        
        return len(relevant_files) > 0
    
    def _generate_summary(self, issues: List[ValidationIssue]) -> str:
        """Generate summary for CI output"""
        
        if not issues:
            return "✅ All data contracts are valid"
        
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]
        warnings = [i for i in issues if i.severity == ValidationSeverity.WARNING]
        
        if errors:
            return f"❌ {len(errors)} critical issues found (blocking)"
        elif warnings:
            return f"⚠️  {len(warnings)} warnings found (non-blocking)"
        else:
            return "ℹ️  Minor issues found"
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Default configuration"""
        return {
            'version': '1.0',
            'data_source': {
                'type': 'dbt',
                'config': {'project_path': '.'}
            },
            'api_framework': {
                'type': 'fastapi',
                'config': {'models_module': 'app.models'}
            },
            'validation': {
                'rules': {
                    'missing_tables': 'error',
                    'missing_columns': 'error',
                    'type_mismatches': 'warning'
                }
            }
        }

# CI/CD Detection and Integration
def get_validation_context() -> Dict[str, Any]:
    """Detect CI/CD environment and create validation context"""
    
    # GitHub Actions
    if os.getenv('GITHUB_ACTIONS'):
        return {
            'mode': ValidationMode.DIFF,
            'changed_files': get_github_changed_files(),
            'is_ci': True,
            'environment': 'github_actions'
        }
    
    # Pre-commit hook
    elif os.getenv('PRE_COMMIT'):
        return {
            'mode': ValidationMode.FAST,
            'changed_files': get_git_staged_files(),
            'is_ci': True,
            'environment': 'pre_commit'
        }
    
    # Local development
    else:
        return {
            'mode': ValidationMode.FULL,
            'changed_files': [],
            'is_ci': False,
            'environment': 'local'
        }

def get_github_changed_files() -> List[str]:
    """Get changed files in GitHub Actions"""
    import subprocess
    try:
        result = subprocess.run([
            'git', 'diff', '--name-only', 
            f"origin/{os.getenv('GITHUB_BASE_REF', 'main')}...HEAD"
        ], capture_output=True, text=True)
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except:
        return []

def get_git_staged_files() -> List[str]:
    """Get staged files for pre-commit"""
    import subprocess
    try:
        result = subprocess.run([
            'git', 'diff', '--cached', '--name-only'
        ], capture_output=True, text=True)
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except:
        return []

# Output Formatters
def output_for_github_actions(result: Dict[str, Any]):
    """Output results in GitHub Actions format"""
    
    if result['success']:
        print("✅ Contract validation passed")
        print(f"::notice::Validation completed in {result['duration']:.2f}s - {result['summary']}")
    else:
        print("❌ Contract validation failed")
        print(f"::error::Validation failed - {result['summary']}")
        
        # Add annotations for each issue
        for issue_dict in result['issues']:
            issue = ValidationIssue.from_dict(issue_dict)
            if issue.severity == ValidationSeverity.ERROR:
                file_annotation = f"file={issue.file_path}" if issue.file_path else ""
                print(f"::error {file_annotation}::{issue.message}")
            else:
                file_annotation = f"file={issue.file_path}" if issue.file_path else ""
                print(f"::warning {file_annotation}::{issue.message}")

def output_for_terminal(result: Dict[str, Any]):
    """Output results for terminal/pre-commit"""
    
    if result['success']:
        print(f"✅ Contract validation passed ({result['duration']:.2f}s)")
        return
    
    print(f"❌ Contract validation failed ({result['duration']:.2f}s)")
    print()
    
    errors = [issue for issue in result['issues'] if issue['severity'] == 'error']
    warnings = [issue for issue in result['issues'] if issue['severity'] == 'warning']
    
    # Show errors first
    if errors:
        print("🚨 CRITICAL ISSUES (will break APIs):")
        for issue_dict in errors[:5]:  # Show only first 5 errors
            issue = ValidationIssue.from_dict(issue_dict)
            print(f"  💥 {issue.table}: {issue.message}")
            if issue.suggested_fix:
                print(f"     💡 Fix: {issue.suggested_fix}")
            print()
        
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more critical errors")
            print()
    
    # Show warnings if no errors
    if warnings and not errors:
        print("⚠️  WARNINGS (non-blocking):")
        for issue_dict in warnings[:3]:
            issue = ValidationIssue.from_dict(issue_dict)
            print(f"  ⚠️  {issue.table}.{issue.column}: {issue.message}")
        print()
    
    if errors:
        print("🚫 Commit blocked - fix critical issues first")
        print("Run 'retl-validator validate --verbose' for detailed analysis")

# Main CLI Function
def main():
    """Main CLI entry point"""
    
    parser = argparse.ArgumentParser(
        description='Reverse ETL Contract Validator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  retl-validator validate                    # Full validation
  retl-validator validate --fast             # Fast validation for pre-commit
  retl-validator validate --config custom.yml  # Custom config file
  retl-validator init                        # Initialize configuration
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate data contracts')
    validate_parser.add_argument('--config', default='.retl-validator.yml', help='Configuration file path')
    validate_parser.add_argument('--mode', choices=['fast', 'diff', 'full'], help='Validation mode')
    validate_parser.add_argument('--output-format', choices=['terminal', 'github', 'json'], default='terminal')
    validate_parser.add_argument('--fail-on', choices=['error', 'warning'], default='error')
    validate_parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize configuration file')
    init_parser.add_argument('--auto-detect', action='store_true', help='Auto-detect project structure')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'init':
        init_config(args.auto_detect)
        return
    
    # Main validation command
    if args.command == 'validate':
        
        # Get CI/CD context
        context = get_validation_context()
        
        # Override mode if specified
        if args.mode:
            context['mode'] = args.mode
        
        # Run validation
        validator = CICDValidator(args.config)
        result = validator.validate_for_cicd(
            mode=context['mode'],
            changed_files=context.get('changed_files', [])
        )
        
        # Output results based on environment and format
        if args.output_format == 'github' or context['environment'] == 'github_actions':
            output_for_github_actions(result)
        elif args.output_format == 'json':
            print(json.dumps(result, indent=2))
        else:
            output_for_terminal(result)
        
        # Exit with appropriate code
        if not result['success']:
            critical_issues = [i for i in result['issues'] if i['severity'] == 'error']
            if args.fail_on == 'error' and critical_issues:
                sys.exit(1)
            elif args.fail_on == 'warning' and result['issues']:
                sys.exit(1)

def init_config(auto_detect: bool = False):
    """Initialize configuration file"""
    
    config_path = Path('.retl-validator.yml')
    
    if config_path.exists():
        print(f"❌ Configuration file already exists: {config_path}")
        response = input("Overwrite? (y/N): ")
        if response.lower() != 'y':
            return
    
    # Auto-detect project structure
    if auto_detect:
        config = auto_detect_config()
    else:
        config = get_default_config()
    
    # Write configuration
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print(f"✅ Created configuration file: {config_path}")
    print("\nNext steps:")
    print("1. Review and customize the configuration")
    print("2. Add pre-commit hook: pre-commit install")
    print("3. Add GitHub Actions workflow")
    print("4. Run: retl-validator validate")

def auto_detect_config() -> Dict[str, Any]:
    """Auto-detect project structure and create config"""
    
    config = {
        'version': '1.0',
        'name': 'auto-detected-contracts'
    }
    
    # Detect DBT project
    if Path('dbt_project.yml').exists():
        config['data_source'] = {
            'type': 'dbt',
            'config': {'project_path': '.'}
        }
        print("✅ Detected DBT project")
    elif Path('models').exists():
        config['data_source'] = {
            'type': 'dbt',
            'config': {'project_path': '.'}
        }
        print("✅ Detected DBT models directory")
    
    # Detect FastAPI
    if Path('app/models.py').exists():
        config['api_framework'] = {
            'type': 'fastapi',
            'config': {'models_module': 'app.models'}
        }
        print("✅ Detected FastAPI models in app/models.py")
    elif Path('models.py').exists():
        config['api_framework'] = {
            'type': 'fastapi',
            'config': {'models_module': 'models'}
        }
        print("✅ Detected models.py")
    
    # Add validation rules
    config['validation'] = {
        'mode': 'strict',
        'rules': {
            'missing_tables': 'error',
            'missing_columns': 'error',
            'type_mismatches': 'warning'
        }
    }
    
    return config

def get_default_config() -> Dict[str, Any]:
    """Get default configuration"""
    return {
        'version': '1.0',
        'name': 'my-data-contracts',
        'data_source': {
            'type': 'dbt',
            'config': {'project_path': './dbt_project'}
        },
        'api_framework': {
            'type': 'fastapi',
            'config': {'models_module': 'app.models'}
        },
        'validation': {
            'mode': 'strict',
            'rules': {
                'missing_tables': 'error',
                'missing_columns': 'error',
                'type_mismatches': 'warning'
            }
        }
    }

if __name__ == "__main__":
    main()