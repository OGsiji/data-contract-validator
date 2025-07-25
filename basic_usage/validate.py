"""
Basic usage example of the data contract validator.
"""

from data_contract_validator import ContractValidator
from data_contract_validator.extractors import DBTExtractor, FastAPIExtractor

def main():
    """Example of basic validation."""
    
    print("🔍 Basic Contract Validation Example")
    print("=" * 40)
    
    # Initialize extractors
    dbt = DBTExtractor(project_path="./dbt_project")
    fastapi = FastAPIExtractor.from_local_file("./fastapi_app/models.py")
    
    # Create validator
    validator = ContractValidator(source=dbt, target=fastapi)
    
    # Run validation
    result = validator.validate()
    
    # Print results
    print(f"\nValidation Result: {'✅ PASSED' if result.success else '❌ FAILED'}")
    print(f"Total issues: {len(result.issues)}")
    print(f"Critical issues: {len(result.critical_issues)}")
    
    if result.critical_issues:
        print("\n🚨 Critical Issues:")
        for issue in result.critical_issues:
            print(f"  • {issue.table}.{issue.column}: {issue.message}")
            if issue.suggested_fix:
                print(f"    Fix: {issue.suggested_fix}")
    
    return result.success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

