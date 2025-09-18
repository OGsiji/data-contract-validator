# examples/directory_usage/validate_directory.py
"""
Complete example showing directory-based FastAPI model validation.
"""

import sys
from pathlib import Path

# Add the package to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from data_contract_validator.core.validator import ContractValidator
from data_contract_validator.extractors.dbt import DBTExtractor
from data_contract_validator.extractors.fastapi import FastAPIExtractor


def main():
    """Example of directory-based validation."""

    print("🔍 Directory-Based Contract Validation Example")
    print("=" * 50)

    try:
        # Setup directory structure
        setup_example_project()

        print("\n📁 Project Structure:")
        print("   dbt_project/")
        print("     ├── dbt_project.yml")
        print("     └── models/")
        print("         ├── users.sql")
        print("         ├── orders.sql")
        print("         └── analytics.sql")
        print("   fastapi_app/")
        print("     └── models/")
        print("         ├── user.py")
        print("         ├── order.py")
        print("         └── analytics.py")

        # Initialize extractors
        print("\n📊 Initializing extractors...")
        dbt = DBTExtractor(project_path="./dbt_project")

        # Use directory-based FastAPI extractor
        fastapi = FastAPIExtractor.from_local_directory("./fastapi_app/models")

        # Create validator
        print("🔧 Creating validator...")
        validator = ContractValidator(source_extractor=dbt, target_extractor=fastapi)

        # Run validation
        print("🔍 Running validation...")
        result = validator.validate()

        # Print detailed results
        print(
            f"\n📊 Validation Result: {'✅ PASSED' if result.success else '❌ FAILED'}"
        )
        print(f"   Total issues: {len(result.issues)}")
        print(f"   Critical issues: {len(result.critical_issues)}")
        print(f"   Warnings: {len(result.warnings)}")

        # Show schema details
        print(f"\n📋 Schemas Found:")
        print(f"   DBT Models: {len(result.source_schemas)}")
        for name, schema in result.source_schemas.items():
            print(f"      {name}: {len(schema.columns)} columns")

        print(f"   FastAPI Models: {len(result.target_schemas)}")
        for name, schema in result.target_schemas.items():
            print(f"      {name}: {len(schema.columns)} columns ({schema.source})")

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
            for issue in result.warnings[:5]:
                print(f"  ⚠️  {issue.table}.{issue.column}: {issue.message}")

            if len(result.warnings) > 5:
                print(f"  ... and {len(result.warnings) - 5} more warnings")

        if result.success:
            print("\n🎉 Validation passed! Your API contracts are compatible.")
            print(
                "💡 All FastAPI models have matching DBT tables with required columns."
            )
        else:
            print("\n💥 Validation failed! Fix the critical issues above.")
            print("💡 Add missing columns to DBT models or update FastAPI models.")

        return result.success

    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback

        traceback.print_exc()
        return False


def setup_example_project():
    """Create a realistic example project structure."""

    # Create DBT project structure
    dbt_dir = Path("./dbt_project")
    models_dir = dbt_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # Create dbt_project.yml
    dbt_project_content = """name: 'directory_example'
version: '1.0.0'
config-version: 2

model-paths: ["models"]
target-path: "target"

models:
  directory_example:
    materialized: view
"""
    (dbt_dir / "dbt_project.yml").write_text(dbt_project_content)

    # Create DBT models
    users_sql = """-- Users model
select
    id as user_id,
    email,
    first_name,
    last_name,
    created_at,
    is_active,
    phone_number
from raw_users
"""
    (models_dir / "users.sql").write_text(users_sql)

    orders_sql = """-- Orders model  
select
    id as order_id,
    user_id,
    total_amount,
    order_status,
    created_at,
    updated_at,
    shipping_address
from raw_orders
"""
    (models_dir / "orders.sql").write_text(orders_sql)

    analytics_sql = """-- User analytics model
select
    user_id,
    total_orders,
    total_revenue,
    avg_order_value,
    last_order_date,
    customer_lifetime_value,
    user_tier,
    acquisition_channel
from user_analytics_raw
"""
    (models_dir / "analytics.sql").write_text(analytics_sql)

    # Create FastAPI models directory structure
    fastapi_dir = Path("./fastapi_app")
    fastapi_models_dir = fastapi_dir / "models"
    fastapi_models_dir.mkdir(parents=True, exist_ok=True)

    # Create FastAPI model files
    user_models = '''"""
User-related Pydantic models.
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
    # phone_number: str  # ← Missing in API model!

class UserProfile(BaseModel):
    """Extended user profile."""
    user_id: str
    email: str
    first_name: str
    last_name: str
    full_name: str  # ← This won't exist in DBT!
    bio: Optional[str] = None
'''
    (fastapi_models_dir / "user.py").write_text(user_models)

    order_models = '''"""
Order-related Pydantic models.
"""

from pydantic import BaseModel
from datetime import datetime
from decimal import Decimal

class Order(BaseModel):
    """Order model - should match DBT orders table."""
    order_id: str
    user_id: str
    total_amount: Decimal  # DBT has float, API expects Decimal
    order_status: str
    created_at: datetime
    updated_at: datetime
    # shipping_address: str  # ← Missing in API model!

class OrderSummary(BaseModel):
    """Order summary for dashboard."""
    order_id: str
    user_id: str
    total_amount: Decimal
    order_status: str
    days_since_order: int  # ← This won't exist in DBT!
'''
    (fastapi_models_dir / "order.py").write_text(order_models)

    analytics_models = '''"""
Analytics-related Pydantic models.
"""

from pydantic import BaseModel
from datetime import datetime
from decimal import Decimal

class UserAnalytics(BaseModel):
    """User analytics - should match DBT analytics table."""
    user_id: str
    total_orders: int
    total_revenue: Decimal  # DBT has float, API expects Decimal
    avg_order_value: Decimal
    last_order_date: datetime
    customer_lifetime_value: Decimal
    user_tier: str
    # acquisition_channel: str  # ← Missing in API model!
'''
    (fastapi_models_dir / "analytics.py").write_text(analytics_models)

    print("✅ Created example project structure:")
    print("   - dbt_project/ with 3 SQL models")
    print("   - fastapi_app/models/ with 3 Python model files")
    print("   - Intentional mismatches for demonstration")


if __name__ == "__main__":
    success = main()
    print(f"\n{'='*50}")
    print(f"Example completed: {'✅ PASSED' if success else '❌ FAILED'}")

    if not success:
        print("\n💡 This failure is intentional to show validation in action!")
        print("   - Missing columns in API models")
        print("   - Type mismatches (float vs Decimal)")
        print("   - Extra columns in API models")

    sys.exit(0)  # Always exit 0 for demo purposes
