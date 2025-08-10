"""
Data Contract Validator

Prevent production API breaks by validating data contracts between 
your data pipelines and API frameworks.
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .core.validator import ContractValidator
from .core.models import ValidationResult, ValidationIssue, IssueSeverity
from .extractors.dbt import DBTExtractor
from .extractors.fastapi import FastAPIExtractor

__all__ = [
    "ContractValidator",
    "ValidationResult",
    "ValidationIssue",
    "IssueSeverity",
    "DBTExtractor",
    "FastAPIExtractor",
]
