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

    def test_incomplete_source_does_not_hard_fail_missing_column(self):
        """A missing column on an incomplete (SELECT *) source must not be critical."""
        source_extractor = Mock()
        target_extractor = Mock()

        source_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[{"name": "id", "type": "varchar", "required": True}],
                source="dbt_sqlglot",
                metadata={"confidence": "medium", "complete": False},
            )
        }
        target_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[
                    {"name": "id", "type": "varchar", "required": True},
                    {"name": "email", "type": "varchar", "required": True},
                ],
                source="test",
            )
        }

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        # Build is NOT blocked, but the user is warned to verify manually.
        assert result.success is True
        assert len(result.critical_issues) == 0
        assert any("email" in w.message for w in result.warnings)

    def test_canonical_types_avoid_false_mismatch(self):
        """dbt 'varchar' vs Pydantic 'str' must not produce a type-mismatch warning."""
        source_extractor = Mock()
        target_extractor = Mock()

        source_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[
                    {
                        "name": "email",
                        "type": "varchar",
                        "canonical_type": "string",
                        "required": True,
                    }
                ],
                source="dbt_catalog",
                metadata={"confidence": "high", "complete": True},
            )
        }
        target_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[
                    {
                        "name": "email",
                        "type": "str",
                        "canonical_type": "string",
                        "required": True,
                    }
                ],
                source="test",
            )
        }

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        assert result.success is True
        assert len(result.issues) == 0

    def test_normalized_name_matching(self):
        """userId (target) should match user_id (source) without a missing-column error."""
        source_extractor = Mock()
        target_extractor = Mock()

        source_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[{"name": "user_id", "type": "varchar", "required": True}],
                source="dbt_catalog",
                metadata={"confidence": "high", "complete": True},
            )
        }
        target_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[{"name": "userId", "type": "str", "required": True}],
                source="test",
            )
        }

        validator = ContractValidator(source_extractor, target_extractor)
        result = validator.validate()

        assert result.success is True
        assert len(result.critical_issues) == 0


class TestPluralSingularAutoMatch:
    """dbt plural models should auto-match singular Pydantic classes."""

    def test_plural_source_matches_singular_target_without_mapping(self):
        source_extractor = Mock()
        target_extractor = Mock()

        # dbt model is plural 'users'; Pydantic 'User' normalizes to 'user'.
        source_extractor.extract_schemas.return_value = {
            "users": Schema(
                name="users",
                columns=[{"name": "user_id", "type": "varchar", "required": True}],
                source="dbt_catalog",
                metadata={"confidence": "high", "complete": True},
            )
        }
        target_extractor.extract_schemas.return_value = {
            "user": Schema(
                name="user",
                columns=[{"name": "user_id", "type": "str", "required": True}],
                source="test",
            )
        }

        result = ContractValidator(source_extractor, target_extractor).validate()

        assert result.success is True
        assert len(result.critical_issues) == 0


class TestExplicitMapping:
    """Test the explicit table/column mapping config."""

    def _extractors(self, source_schemas, target_schemas):
        source_extractor = Mock()
        target_extractor = Mock()
        source_extractor.extract_schemas.return_value = source_schemas
        target_extractor.extract_schemas.return_value = target_schemas
        return source_extractor, target_extractor

    def test_table_mapping_resolves_differently_named_models(self):
        source_extractor, target_extractor = self._extractors(
            {
                "user_analytics_summary": Schema(
                    name="user_analytics_summary",
                    columns=[{"name": "user_id", "type": "varchar", "required": True}],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )
            },
            {
                "user_analytics": Schema(
                    name="user_analytics",
                    columns=[{"name": "user_id", "type": "str", "required": True}],
                    source="test",
                )
            },
        )

        # Without mapping: the names don't match -> missing table (critical).
        no_map = ContractValidator(source_extractor, target_extractor).validate()
        assert no_map.success is False
        assert no_map.critical_issues[0].category == "Missing Table"

        # With mapping: target 'user_analytics' -> source 'user_analytics_summary'.
        mapping = {"tables": {"user_analytics": "user_analytics_summary"}}
        mapped = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()
        assert mapped.success is True
        assert len(mapped.issues) == 0

    def test_column_mapping_resolves_renamed_columns(self):
        source_extractor, target_extractor = self._extractors(
            {
                "users": Schema(
                    name="users",
                    columns=[
                        {
                            "name": "customer_identifier",
                            "type": "varchar",
                            "required": True,
                        }
                    ],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )
            },
            {
                "users": Schema(
                    name="users",
                    columns=[{"name": "user_id", "type": "str", "required": True}],
                    source="test",
                )
            },
        )

        mapping = {"columns": {"users": {"user_id": "customer_identifier"}}}
        result = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()

        assert result.success is True
        assert len(result.critical_issues) == 0

    def test_exclude_skips_a_target_table_with_no_source_model(self):
        """A target table that's genuinely populated by something other than
        dbt (e.g. a Kafka stream) has no source model on purpose -- that's
        not inferable from the code, so it must be excluded explicitly
        rather than producing a permanent, unfixable 'missing table'."""
        source_extractor, target_extractor = self._extractors(
            {},  # no source models at all
            {
                "feed_interaction": Schema(
                    name="feed_interaction",
                    columns=[{"name": "id", "type": "str", "required": True}],
                    source="test",
                )
            },
        )

        # Without exclude: no matching source model -> missing table (critical).
        no_exclude = ContractValidator(source_extractor, target_extractor).validate()
        assert no_exclude.success is False
        assert no_exclude.critical_issues[0].category == "Missing Table"

        # With exclude: the table is skipped entirely, no issue raised.
        mapping = {"exclude": ["feed_interaction"]}
        excluded = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()
        assert excluded.success is True
        assert len(excluded.issues) == 0

    def test_exclude_does_not_affect_other_tables(self):
        """Excluding one table must not suppress real issues on others."""
        source_extractor, target_extractor = self._extractors(
            {},
            {
                "feed_interaction": Schema(
                    name="feed_interaction",
                    columns=[{"name": "id", "type": "str", "required": True}],
                    source="test",
                ),
                "orders": Schema(
                    name="orders",
                    columns=[{"name": "order_id", "type": "str", "required": True}],
                    source="test",
                ),
            },
        )

        mapping = {"exclude": ["feed_interaction"]}
        result = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()

        assert result.success is False
        assert len(result.critical_issues) == 1
        assert result.critical_issues[0].table == "orders"

    def test_exclude_is_normalized_like_table_mapping(self):
        """Exclude entries should match case/style-insensitively, same as
        `mapping.tables`, so 'FeedInteraction' and 'feed_interaction' are
        treated as the same table."""
        source_extractor, target_extractor = self._extractors(
            {},
            {
                "feed_interaction": Schema(
                    name="feed_interaction",
                    columns=[{"name": "id", "type": "str", "required": True}],
                    source="test",
                )
            },
        )

        mapping = {"exclude": ["FeedInteraction"]}
        result = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()

        assert result.success is True
        assert len(result.issues) == 0
