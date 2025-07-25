# examples/basic_usage/validate.py
"""
Basic usage example that actually works.
"""

import sys
from pathlib import Path

# Add the package to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from data_contract_validator.core.validator import ContractValidator
from data_contract_validator.extractors.dbt import DBTExtractor
from data_contract_validator.extractors.fastapi import FastAPIExtractor

def main():
    """Example of basic validation."""
    
    print("🔍 Basic Contract Validation Example")
    print("=" * 40)
    
    try:
        # Check if we have example files
        dbt_project_path = Path("./dbt_project")
        fastapi_models_path = Path("./fastapi_app/models.py")
        
        if not dbt_project_path.exists():
            print("❌ No DBT project found at ./dbt_project")
            print("💡 Creating minimal example files...")
            create_example_files()
        
        if not fastapi_models_path.exists():
            print("❌ No FastAPI models found at ./fastapi_app/models.py")
            print("💡 Creating minimal example files...")
            create_example_files()
        
        # Initialize extractors
        print("\n📊 Initializing extractors...")
        dbt = DBTExtractor(project_path="./dbt_project")
        fastapi = FastAPIExtractor.from_local_file("./fastapi_app/models.py")
        
        # Create validator
        print("🔧 Creating validator...")
        validator = ContractValidator(source_extractor=dbt, target_extractor=fastapi)
        
        # Run validation
        print("🔍 Running validation...")
        result = validator.validate()
        
        # Print results
        print(f"\n📊 Validation Result: {'✅ PASSED' if result.success else '❌ FAILED'}")
        print(f"   Total issues: {len(result.issues)}")
        print(f"   Critical issues: {len(result.critical_issues)}")
        print(f"   Warnings: {len(result.warnings)}")
        
        if result.critical_issues:
            print("\n🚨 Critical Issues:")
            for issue in result.critical_issues:
                print(f"  💥 {issue.table}")
                if issue.column:
                    print(f"      Column: {issue.column}")
                print(f"      Problem: {issue.message}")
                if issue.suggested_fix:
                    print(f"      Fix: {issue.suggested_fix}")
                print()
        
        if result.warnings and not result.critical_issues:
            print("\n⚠️  Warnings:")
            for issue in result.warnings[:3]:
                print(f"  ⚠️  {issue.table}.{issue.column}: {issue.message}")
            
            if len(result.warnings) > 3:
                print(f"  ... and {len(result.warnings) - 3} more warnings")
        
        if result.success:
            print("\n🎉 Validation passed! Your API contracts are compatible.")
        else:
            print("\n💥 Validation failed! Fix the critical issues above.")
        
        return result.success
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_example_files():
    """Create minimal example files for testing."""
    
    # Create DBT project structure
    dbt_dir = Path("./dbt_project")
    models_dir = dbt_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Create dbt_project.yml
    dbt_project_content = """name: 'example_project'
version: '1.0.0'
config-version: 2

model-paths: ["models"]
target-path: "target"

models:
  example_project:
    materialized: view
"""
    
    (dbt_dir / "dbt_project.yml").write_text(dbt_project_content)
    
    # Create simple DBT model
    dbt_model_content = """-- Simple users model
select
    id as user_id,
    email,
    first_name,
    last_name,
    created_at,
    is_active
from raw_users
"""
    
    (models_dir / "users.sql").write_text(dbt_model_content)
    
    # Create FastAPI app structure
    fastapi_dir = Path("./fastapi_app")
    fastapi_dir.mkdir(exist_ok=True)
    
    # Create FastAPI models
    fastapi_models_content = '''"""
Example FastAPI models using Pydantic.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class User(BaseModel):
    """User model - should match DBT users table."""
    user_id: str
    email: str
    first_name: str
    last_name: str
    created_at: datetime
    is_active: bool

class UserProfile(BaseModel):
    """User profile - might have missing fields."""
    user_id: str
    email: str
    full_name: str  # This won't exist in DBT!
    bio: Optional[str] = None
'''
    
    (fastapi_dir / "models.py").write_text(fastapi_models_content)
    
    print("✅ Created example files:")
    print("   - dbt_project/dbt_project.yml")
    print("   - dbt_project/models/users.sql")
    print("   - fastapi_app/models.py")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)