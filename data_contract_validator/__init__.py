"""
Data Contract Validator

Prevent production API breaks by validating data contracts between
your data pipelines and API frameworks.
"""

__version__ = "1.1.2"
__author__ = "Ogunniran Siji"
__email__ = "ogunniransiji@gmail.com"

from .core.validator import ContractValidator
from .core.models import ValidationResult, ValidationIssue, IssueSeverity, Schema
from .core.types import CanonicalType
from .extractors.dbt import DBTExtractor
from .extractors.fastapi import FastAPIExtractor

__all__ = [
    "ContractValidator",
    "ValidationResult",
    "ValidationIssue",
    "IssueSeverity",
    "Schema",
    "CanonicalType",
    "DBTExtractor",
    "FastAPIExtractor",
]
