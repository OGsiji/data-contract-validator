from setuptools import setup, find_packages
import os

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
try:
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
except FileNotFoundError:
    requirements = [
        "pydantic>=2.0.0",
        "PyYAML>=6.0", 
        "requests>=2.25.0",
        "click>=8.0.0",
    ]

setup(
    name="reverse-etl-validator",
    version="0.1.0",
    author="Ogunniran Siji",
    author_email="ogunniransiji@gmail.com",
    description="Prevent production breaks by validating data contracts between DBT models and API frameworks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OGsiji/retl_validator",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.991",
            "pre-commit>=2.20.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "contract-validator=data_contract_validator.cli:main",
            "dbt-contract-validator=data_contract_validator.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "data_contract_validator": [
            "templates/*.yml",
            "templates/*.yaml",
        ],
    },
    keywords=[
        "dbt", "fastapi", "contract-testing", "api-validation", 
        "data-engineering", "schema-validation", "ci-cd", "devops"
    ],
    project_urls={
        "Bug Reports": "https://github.com/your-org/data-contract-validator/issues",
        "Source": "https://github.com/your-org/data-contract-validator",
        "Documentation": "https://github.com/your-org/data-contract-validator/blob/main/docs/",
    },
)

