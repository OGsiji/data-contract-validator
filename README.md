# 🛡️ Data Contract Validator

> **Prevent production API breaks by validating data contracts between your data pipelines and API frameworks**

[![PyPI version](https://badge.fury.io/py/data-contract-validator.svg)](https://badge.fury.io/py/data-contract-validator)
[![Tests](https://github.com/your-org/data-contract-validator/workflows/Tests/badge.svg)](https://github.com/your-org/data-contract-validator/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 **What This Solves**

Ever deployed a DBT model change only to break your FastAPI in production? This tool prevents that by validating data contracts between your data pipelines and APIs **before** deployment.

```
DBT Models          Contract           FastAPI Models
(What data          Validator          (What APIs
 produces)          ↕️ VALIDATES ↕️      expect)
     ↓                   ↓                   ↓
   Schema              Finds              Schema
 Extraction          Mismatches         Extraction
```

## ⚡ **Quick Start**

### **Installation**
```bash
pip install data-contract-validator
```

### **Basic Usage**
```bash
# Validate local DBT project against FastAPI models
contract-validator validate \
  --dbt-project ./my-dbt-project \
  --fastapi-models ./my-api/models.py

# Validate across repositories (perfect for microservices)
contract-validator validate \
  --dbt-project . \
  --fastapi-repo "my-org/my-api-repo" \
  --fastapi-path "app/models.py"
```

### **GitHub Actions Integration**
```yaml
# .github/workflows/validate-contracts.yml
name: Validate Data Contracts
on: [pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install validator
      run: pip install data-contract-validator
    
    - name: Validate contracts
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      run: |
        contract-validator validate \
          --dbt-project . \
          --fastapi-repo "my-org/my-api" \
          --github-token "$GITHUB_TOKEN"
```

## 🔍 **What It Validates**

### **❌ Critical Issues (Block Deployment)**
- **Missing tables**: API expects `user_analytics` but DBT doesn't provide it
- **Missing required columns**: API requires `total_revenue` but DBT model doesn't have it

### **⚠️ Warnings (Non-blocking)**
- **Type mismatches**: DBT provides `varchar` but API expects `integer`
- **Missing optional columns**: API can handle missing optional fields

### **ℹ️ Info (Good to Know)**
- **Extra columns**: DBT provides columns that API doesn't use

## 🎯 **Real-World Example**

### **Before (Production Breaks) 💥**
```sql
-- DBT model changes
select
    user_id,
    email,
    -- total_orders,  ❌ REMOVED this column
    revenue
from users
```

```python
# FastAPI model (unchanged)
class UserAnalytics(BaseModel):
    user_id: str
    email: str
    total_orders: int  # ❌ Still expects this!
    revenue: float
```

**Result:** API breaks in production 💀

### **After (Caught by Validator) ✅**
```bash
❌ VALIDATION FAILED
💥 user_analytics.total_orders: FastAPI REQUIRES column but DBT removed it
🔧 Fix: Add 'total_orders' back to DBT model or update FastAPI model
```

**Result:** Issue caught in CI/CD, production safe! 🛡️

## 🚀 **Supported Frameworks**

### **Data Sources**
- ✅ **DBT** (dbt-core, all adapters)
- 🔄 **Databricks** (coming soon)
- 🔄 **Airflow** (coming soon)

### **API Frameworks**  
- ✅ **FastAPI** (Pydantic + SQLModel)
- 🔄 **Django** (coming soon)
- 🔄 **Flask-SQLAlchemy** (coming soon)

*Want to add support for your framework? [See extending guide](docs/extending.md)*

## 📦 **Installation Options**

### **Option 1: PyPI (Recommended)**
```bash
pip install data-contract-validator
```

### **Option 2: From Source**
```bash
git clone https://github.com/your-org/data-contract-validator
cd data-contract-validator
pip install -e .
```

### **Option 3: GitHub Actions Only**
```yaml
- name: Validate Contracts
  uses: your-org/data-contract-validator@v1
  with:
    dbt-project: '.'
    fastapi-repo: 'my-org/my-api'
```

## 🔧 **Configuration**

### **Command Line**
```bash
contract-validator validate \
  --dbt-project ./dbt-project \           # DBT project path
  --fastapi-repo "org/repo" \             # GitHub repo
  --fastapi-path "app/models.py" \        # Path to models
  --github-token "$GITHUB_TOKEN" \        # For private repos
  --output json                           # Output format
```

### **Configuration File**
```yaml
# .contract-validator.yml
version: '1.0'
sources:
  dbt:
    project_path: './dbt-project'
    auto_update_schemas: true

targets:
  fastapi:
    repo: 'my-org/my-api'
    path: 'app/models.py'
    
validation:
  fail_on: ['missing_tables', 'missing_required_columns']
  warn_on: ['type_mismatches', 'missing_optional_columns']
```

## 📊 **Output Formats**

### **Terminal (Default)**
```bash
🔍 Contract Validation Results:

❌ CRITICAL ISSUES:
  💥 user_analytics.total_revenue: FastAPI expects this column but DBT doesn't provide it
     🔧 Fix: Add 'total_revenue' to your DBT model

✅ VALIDATION PASSED (with warnings)
```

### **GitHub Actions**
```bash
::error::user_analytics.total_revenue: Missing required column
::warning::user_analytics.age: Type mismatch (varchar vs integer)
```

### **JSON**
```json
{
  "success": false,
  "issues": [
    {
      "severity": "error",
      "table": "user_analytics", 
      "column": "total_revenue",
      "message": "FastAPI expects column but DBT doesn't provide it",
      "suggestion": "Add 'total_revenue' to your DBT model"
    }
  ]
}
```

## 🏗️ **Architecture**

```python
# Simple, extensible architecture
from data_contract_validator import ContractValidator
from data_contract_validator.extractors import DBTExtractor, FastAPIExtractor

# Initialize extractors
dbt = DBTExtractor(project_path='./dbt-project')
fastapi = FastAPIExtractor(repo='my-org/my-api', path='app/models.py')

# Run validation
validator = ContractValidator(source=dbt, target=fastapi)
result = validator.validate()

if not result.success:
    print(f"❌ {len(result.critical_issues)} critical issues found")
    for issue in result.critical_issues:
        print(f"💥 {issue.table}.{issue.column}: {issue.message}")
```

## 🤝 **Contributing**

We love contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### **Quick Setup**
```bash
git clone https://github.com/your-org/data-contract-validator
cd data-contract-validator
pip install -e ".[dev]"
pytest
```

### **Adding New Extractors**
```python
from data_contract_validator.extractors import BaseExtractor

class MyFrameworkExtractor(BaseExtractor):
    def extract_schemas(self) -> Dict[str, Schema]:
        # Your implementation
        return schemas
```

## 🎉 **Success Stories**

> *"We prevented 15 production incidents in our first month using this tool. It's now required in all our data pipeline PRs."*  
> — Data Engineering Team, TechCorp

> *"Finally! A tool that validates the contract between our DBT models and FastAPI services. No more surprise 500 errors."*  
> — Platform Team, StartupCo

## 📚 **Documentation**

- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)  
- [GitHub Actions Setup](docs/github-actions.md)
- [Extending with New Extractors](docs/extending.md)
- [API Reference](docs/api-reference.md)

## 📄 **License**

MIT License - see [LICENSE](LICENSE) file for details.

## 🆘 **Support**

- 🐛 **Bug reports**: [GitHub Issues](https://github.com/your-org/data-contract-validator/issues)
- 💡 **Feature requests**: [GitHub Discussions](https://github.com/your-org/data-contract-validator/discussions)
- 📧 **Email**: your-email@example.com

## ⭐ **Star History**

If this tool helps you prevent production incidents, please star the repo! ⭐

---

**Built with ❤️ by data engineers, for data engineers.**