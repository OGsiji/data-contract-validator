# 🛡️ Data Contract Validator

> **Prevent production API breaks by validating data contracts between your data pipelines and API frameworks**

[![PyPI version](https://badge.fury.io/py/data-contract-validator.svg)](https://badge.fury.io/py/data-contract-validator)
[![Tests](https://github.com/OGsiji/data-contract-validator/workflows/Tests/badge.svg)](https://github.com/OGsiji/data-contract-validator/actions)
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

### **30-Second Setup**
```bash
# 1. Initialize in your project
contract-validator init --interactive

# 2. Test setup
contract-validator test

# 3. Validate contracts
contract-validator validate

# 4. Commit and push - you're protected! 🛡️
```

### **Basic Usage**
```bash
# Validate local DBT project against FastAPI models
contract-validator validate \
  --dbt-project ./my-dbt-project \
  --fastapi-local ./my-api/models.py

# Validate across repositories (microservices)
contract-validator validate \
  --dbt-project . \
  --fastapi-repo "my-org/my-api-repo" \
  --fastapi-path "app/models.py"
```

## 🔍 **Real Example: Production Validation**

**Actual output from a production analytics project:**

```bash
$ contract-validator validate

🔍 Starting contract validation...
📊 Extracting source schemas...
   ✅ Found 14 DBT models (user_analytics_summary: 54 columns)
🎯 Extracting target schemas...  
   ✅ Found 3 FastAPI models
🔍 Validating schema compatibility...

🛡️ Results:
✅ PASSED - 0 critical issues (no production breaks!)
⚠️  42 warnings (type mismatches to review)

Issues caught:
⚠️  user_analytics_summary.age_years: source 'varchar' vs target 'integer'
⚠️  user_analytics_summary.is_verified: source 'varchar' vs target 'boolean'
⚠️  user_analytics_summary.user_created_at: source 'varchar' vs target 'timestamp'

🎉 Your API contracts are protected!
```

## 🚨 **What It Prevents**

### **Before Data Contract Validation:**
```sql
-- Analytics team changes DBT model
select
    user_id,
    email,
    -- total_orders,  ❌ REMOVED this column
    revenue
from users
```

```python
# API team's FastAPI model (unchanged)
class UserAnalytics(BaseModel):
    user_id: str
    email: str
    total_orders: int  # ❌ Still expects this!
    revenue: float
```

**Result:** 💥 **Production API breaks**, angry customers, 2AM debugging

### **After Data Contract Validation:**
```bash
$ git push

❌ VALIDATION FAILED
💥 user_analytics.total_orders: FastAPI REQUIRES column but DBT removed it
🔧 Fix: Add 'total_orders' back to DBT model or update FastAPI model

# Push blocked until fixed ✋
```

**Result:** 🛡️ **Production protected**, issues caught in CI/CD

## 🛠️ **Pre-commit Integration**

### **Automatic Setup (Recommended)**
```bash
# Initialize with pre-commit support
contract-validator init --interactive
contract-validator setup-precommit --install-hooks

# Now every commit validates contracts automatically! 🛡️
```

### **Manual Setup**
If you prefer manual setup:

1. **Install pre-commit:**
   ```bash
   pip install pre-commit
   ```

2. **Add to `.pre-commit-config.yaml`:**
   ```yaml
   repos:
     - repo: https://github.com/OGsiji/data-contract-validator
       rev: v1.0.0
       hooks:
         - id: contract-validation
           name: Validate Data Contracts
           files: '^(.*models.*\.(sql|py)|\.retl-validator\.yml|dbt_project\.yml)$'
   ```

3. **Install hooks:**
   ```bash
   pre-commit install
   ```

### **How It Works**
```bash
$ git add models/user_analytics.sql
$ git commit -m "update user analytics model"

# Pre-commit automatically runs:
🔍 Validating Data Contracts...
✅ Contract validation passed
[main abc1234] update user analytics model
```

### **On Validation Failure**
```bash
$ git commit -m "remove important column"

🔍 Validating Data Contracts...
❌ CRITICAL: user_analytics.total_revenue missing
💡 Fix the issue before committing

# Commit blocked until fixed! 🛡️
```

### **Skip Validation (Emergency Only)**
```bash
# Only for emergencies!
git commit -m "emergency fix" --no-verify
```

### **Benefits of Pre-commit Integration**
- ✅ **Catches issues before they reach CI/CD**
- ✅ **Faster feedback loop** (seconds, not minutes)
- ✅ **No broken commits** in your git history
- ✅ **Team protection** - everyone gets validation
- ✅ **Zero configuration** after setup

## 📦 **GitHub Actions Integration**

Add this to `.github/workflows/validate-contracts.yml`:

```yaml
name: 🛡️ Data Contract Validation

on:
  pull_request:
    paths:
      - 'models/**/*.sql'
      - 'dbt_project.yml'
      - '**/*models*.py'

jobs:
  validate-contracts:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Validate contracts
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      run: |
        pip install data-contract-validator
        contract-validator validate
```

**Auto-generated when you run `contract-validator init`!**

## 🔧 **Configuration**

### **Auto-Generated Config (`.retl-validator.yml`)**
```yaml
version: '1.0'
name: 'my-project-contracts'

source:
  dbt:
    project_path: '.'
    auto_compile: true

target:
  fastapi:
    # For GitHub repos
    type: "github"
    repo: "my-org/my-api"
    path: "app/models.py"
    
    # For local files
    # type: "local"
    # path: "../my-api/models.py"

validation:
  fail_on: ['missing_tables', 'missing_required_columns']
  warn_on: ['type_mismatches', 'missing_optional_columns']
```

### **Command Line Options**
```bash
contract-validator validate \
  --dbt-project ./dbt-project \           # DBT project path
  --fastapi-repo "org/repo" \             # GitHub repo
  --fastapi-path "app/models.py" \        # Path to models
  --github-token "$GITHUB_TOKEN" \        # For private repos
  --output json                           # json, terminal, github
```

## 🚀 **Supported Frameworks**

### **Data Sources ✅**
- **DBT** (all adapters: Snowflake, BigQuery, Redshift, etc.)

### **API Frameworks ✅**  
- **FastAPI** (Pydantic + SQLModel)

### **Coming Soon 🔄**
- Django, Flask-SQLAlchemy
- Databricks, Airflow
- [Request other frameworks](https://github.com/OGsiji/data-contract-validator/issues)

## 🎯 **Output Formats**

### **Terminal (Default)**
```bash
🛡️ Data Contract Validation Results:
Status: ✅ PASSED
Critical: 0 | Warnings: 5

⚠️  Warnings:
  user_analytics.age: Type mismatch (varchar vs integer)
  user_analytics.country: Type mismatch (integer vs varchar)

🎉 Your API contracts are protected!
```

### **JSON (for CI/CD)**
```json
{
  "success": true,
  "critical_issues": 0,
  "warnings": 5,
  "issues": [
    {
      "severity": "warning",
      "table": "user_analytics", 
      "column": "age",
      "message": "Type mismatch: source 'varchar' vs target 'integer'",
      "suggested_fix": "Update target to expect 'varchar' or fix source type"
    }
  ]
}
```

### **GitHub Actions**
```bash
::warning::user_analytics.age: Type mismatch detected
✅ Contract validation passed - no critical issues
```

## 🏗️ **Architecture**

### **Simple Python API**
```python
from data_contract_validator import ContractValidator, DBTExtractor, FastAPIExtractor

# Initialize extractors
dbt = DBTExtractor(project_path='./dbt-project')
fastapi = FastAPIExtractor.from_github_repo('my-org/my-api', 'app/models.py')

# Run validation
validator = ContractValidator(source=dbt, target=fastapi)
result = validator.validate()

if not result.success:
    print(f"❌ {len(result.critical_issues)} critical issues found")
    for issue in result.critical_issues:
        print(f"💥 {issue.table}.{issue.column}: {issue.message}")
```

### **CLI Interface**
```bash
# Interactive setup
contract-validator init --interactive

# Test configuration
contract-validator test

# Run validation
contract-validator validate

# Setup pre-commit hooks
contract-validator setup-precommit --install-hooks

# Multiple output formats
contract-validator validate --output json
```

## 🔄 **Development Workflow**

### **With Pre-commit (Recommended)**
```bash
# Team workflow with automated validation
git clone your-dbt-project
cd your-dbt-project

# One-time setup for new team members
contract-validator init --interactive
contract-validator setup-precommit --install-hooks

# Protected development workflow:
# 1. Make changes to DBT models
# 2. git add models/my_model.sql
# 3. git commit -m "update model"  # ← Validation runs here automatically
# 4. If validation passes → commit succeeds
# 5. If validation fails → fix issues first
# 6. git push  # ← CI/CD validation as backup
```

### **Manual Workflow**
```bash
# Traditional workflow
# 1. Make changes
# 2. contract-validator validate  # Manual validation
# 3. git commit
# 4. git push
```

## 🤝 **Contributing**

We welcome contributions! This tool is actively used in production.

### **Development Setup**
```bash
git clone https://github.com/OGsiji/data-contract-validator
cd data-contract-validator
pip install -e ".[dev]"
pytest
```

### **Adding New Extractors**
```python
from retl_validator.extractors import BaseExtractor

class MyFrameworkExtractor(BaseExtractor):
    def extract_schemas(self) -> Dict[str, Schema]:
        # Your implementation
        return schemas
```

### **Reporting Issues**
- 🐛 **Bugs**: [GitHub Issues](https://github.com/OGsiji/data-contract-validator/issues)
- 💡 **Features**: [GitHub Discussions](https://github.com/OGsiji/data-contract-validator/discussions)

## 📚 **Documentation**

- **[Quick Start Guide](https://github.com/OGsiji/data-contract-validator#quick-start)** - Get running in 2 minutes
- **[Configuration Reference](https://github.com/OGsiji/data-contract-validator/blob/main/examples)** - All config options
- **[GitHub Actions Setup](https://github.com/OGsiji/data-contract-validator/blob/main/examples/.github_actions)** - CI/CD integration
- **[Examples](https://github.com/OGsiji/data-contract-validator/tree/main/examples)** - Real-world usage
- **[Pre-commit Integration](https://github.com/OGsiji/data-contract-validator#pre-commit-integration)** - Automated validation

## 🎉 **Real-World Usage**

This tool is actively preventing production incidents in:
- **Analytics pipelines** with 50+ DBT models
- **Microservices architectures** with multiple APIs
- **Data engineering teams** using Snowflake, BigQuery, Redshift
- **Cross-repository validation** in large organizations

**Proven to catch:**
- ✅ **Type mismatches** (varchar vs integer)
- ✅ **Missing columns** (API expects columns DBT doesn't provide)
- ✅ **Schema drift** (gradual model changes)
- ✅ **Breaking changes** before they reach production

## 🛡️ **Multiple Layers of Protection**

1. **Pre-commit hooks**: Immediate feedback (fastest)
2. **CI/CD validation**: Team protection (backup)
3. **Manual validation**: Development testing
4. **Configuration files**: Team standards

This creates a comprehensive safety net for your data contracts.

## 📄 **License**

MIT License - see [LICENSE](https://github.com/OGsiji/data-contract-validator/blob/main/LICENSE) for details.

## 🆘 **Support**

- 🐛 **Issues**: [GitHub Issues](https://github.com/OGsiji/data-contract-validator/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/OGsiji/data-contract-validator/discussions)
- 📧 **Email**: ogunniransiji@gmail.com

## ⭐ **Star the Project**

If this tool helps you prevent production incidents, please ⭐ star the repository!

---

**🛡️ Built by data engineers, for data engineers. Stop breaking production with data changes!**

## 🚀 **Get Started Now**

```bash
pip install data-contract-validator
contract-validator init --interactive
contract-validator setup-precommit --install-hooks
# 2 minutes to production protection with automated validation!
```