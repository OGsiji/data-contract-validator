"""
Tests for the core validator functionality.
"""

import pytest
from unittest.mock import Mock, patch

from data_contract_validator.core.validator import ContractValidator
from data_contract_validator.core.models import ValidationResult, IssueSeverity, Schema


class TestContractValidator:
    """Test the ContractValidator class."""

    def test_initialization(self):
        """Test validator initialization."""
        source_extractor = Mock()
        target_extractor = Mock()

        validator = ContractValidator(source_extractor, target_extractor)

        assert validator.source_extractor == source_extractor
        assert validator.target_extractor == target_extractor
        assert validator.issues == []

    def test_successful_validation(self, sample_schemas):
        """Test successful validation with matching schemas."""
        source_extractor = Mock()
        target_extractor = Mock()

        # Setup mock return values
        source_schemas = {
            "users": Schema(
                name="users",
                columns=sample_schemas["source"]["users"]["columns"],
                source="test",
            )
        }
        target_schemas = {
            "users": Schema(
                name="users",
                columns=sample_schemas["target"]["user"]["columns"],
                source="test",
            )
        }

        source_extractor.extract_schemas.return_value = source_schemas
        target_extractor.extract_schemas.return_value = target_schemas

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        assert isinstance(result, ValidationResult)
        assert result.success == True
        assert len(result.critical_issues) == 0

    def test_missing_table_validation(self):
        """Test validation when target expects missing table."""
        source_extractor = Mock()
        target_extractor = Mock()

        source_extractor.extract_schemas.return_value = {}
        target_extractor.extract_schemas.return_value = {
            "missing_table": Schema(
                name="missing_table",
                columns=[{"name": "id", "type": "varchar", "required": True}],
                source="test",
            )
        }

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        assert result.success == False
        assert len(result.critical_issues) == 1
        assert result.critical_issues[0].table == "missing_table"
        assert result.critical_issues[0].severity == IssueSeverity.CRITICAL

    def test_missing_column_validation(self):
        """Test validation when target expects missing column."""
        source_extractor = Mock()
        target_extractor = Mock()

        source_schemas = {
            "users": Schema(
                name="users",
                columns=[{"name": "id", "type": "varchar", "required": True}],
                source="test",
            )
        }
        target_schemas = {
            "users": Schema(
                name="users",
                columns=[
                    {"name": "id", "type": "varchar", "required": True},
                    {"name": "email", "type": "varchar", "required": True},
                ],
                source="test",
            )
        }

        source_extractor.extract_schemas.return_value = source_schemas
        target_extractor.extract_schemas.return_value = target_schemas

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        assert result.success == False
        assert len(result.critical_issues) == 1
        assert result.critical_issues[0].column == "email"
        assert "email" in result.critical_issues[0].message
