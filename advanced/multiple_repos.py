"""
Example of validating contracts across multiple repositories.
"""

from data_contract_validator import ContractValidator
from data_contract_validator.extractors import DBTExtractor, FastAPIExtractor

def validate_multiple_apis():
    """Validate one DBT project against multiple API repositories."""
    
    # Single DBT source
    dbt = DBTExtractor(project_path="./analytics-dbt")
    
    # Multiple API targets
    apis = [
        ("user-service", FastAPIExtractor.from_github_repo(
            repo="my-org/user-service",
            path="app/models.py"
        )),
        ("order-service", FastAPIExtractor.from_github_repo(
            repo="my-org/order-service", 
            path="src/models.py"
        )),
        ("analytics-api", FastAPIExtractor.from_local_file(
            "./analytics-api/models.py"
        ))
    ]
    
    results = {}
    
    for service_name, api_extractor in apis:
        print(f"\n🔍 Validating {service_name}...")
        
        validator = ContractValidator(source=dbt, target=api_extractor)
        result = validator.validate()
        
        results[service_name] = result
        
        print(f"   {service_name}: {'✅' if result.success else '❌'} "
              f"({len(result.critical_issues)} critical issues)")
    
    # Summary
    print(f"\n📊 Multi-Service Validation Summary:")
    total_services = len(results)
    passing_services = sum(1 for r in results.values() if r.success)
    
    print(f"   Services: {passing_services}/{total_services} passing")
    
    # List failing services
    failing = [name for name, result in results.items() if not result.success]
    if failing:
        print(f"   Failing: {', '.join(failing)}")
    
    return all(result.success for result in results.values())

if __name__ == "__main__":
    success = validate_multiple_apis()
    exit(0 if success else 1)
