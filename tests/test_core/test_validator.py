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

    def test_critical_columns_escalates_a_non_required_missing_column(self):
        """Targets like HubSpot/Salesforce properties have no schema-level
        'required' concept, so every missing column from them defaults to a
        WARNING. mapping.critical_columns lets a human state that a specific
        field is actually load-bearing, without needing the target extractor
        itself to know anything about that."""
        source_extractor, target_extractor = self._extractors(
            {
                "contacts": Schema(
                    name="contacts",
                    columns=[{"name": "firstname", "type": "str", "required": True}],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )
            },
            {
                "contacts": Schema(
                    name="contacts",
                    # required=False mirrors what HubSpotExtractor always emits.
                    columns=[{"name": "email", "type": "string", "required": False}],
                    source="hubspot:contacts",
                )
            },
        )

        # Without the override: HubSpot-style non-required column -> warning only.
        no_override = ContractValidator(source_extractor, target_extractor).validate()
        assert no_override.success is True
        assert len(no_override.warnings) == 1
        assert len(no_override.critical_issues) == 0

        # With the override: the same missing column now fails the build.
        mapping = {"critical_columns": {"contacts": ["email"]}}
        escalated = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()
        assert escalated.success is False
        assert len(escalated.critical_issues) == 1
        assert escalated.critical_issues[0].column == "email"

    def test_critical_columns_is_normalized_and_scoped_per_table(self):
        """Casing/style-insensitive like the other mapping keys, and must not
        leak into escalating a same-named column on a different table."""
        source_extractor, target_extractor = self._extractors(
            {
                "contacts": Schema(
                    name="contacts",
                    columns=[{"name": "firstname", "type": "str", "required": True}],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                ),
                "companies": Schema(
                    name="companies",
                    columns=[{"name": "name", "type": "str", "required": True}],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                ),
            },
            {
                "contacts": Schema(
                    name="contacts",
                    columns=[{"name": "email", "type": "string", "required": False}],
                    source="hubspot:contacts",
                ),
                "companies": Schema(
                    name="companies",
                    columns=[{"name": "email", "type": "string", "required": False}],
                    source="hubspot:companies",
                ),
            },
        )

        mapping = {"critical_columns": {"Contacts": ["Email"]}}
        result = ContractValidator(
            source_extractor, target_extractor, mapping=mapping
        ).validate()

        critical_tables = {i.table for i in result.critical_issues}
        warning_tables = {i.table for i in result.warnings}
        assert critical_tables == {"contacts"}
        assert warning_tables == {"companies"}


class TestDidYouMeanSuggestions:
    """A genuine rename (not a mechanical casing/plural transform) can't be
    bridged by find_match(), so it must still be reported -- but the fix
    should suggest the closest actual source name instead of leaving the
    user to guess."""

    def _extractors(self, source_schemas, target_schemas):
        source_extractor = Mock()
        target_extractor = Mock()
        source_extractor.extract_schemas.return_value = source_schemas
        target_extractor.extract_schemas.return_value = target_schemas
        return source_extractor, target_extractor

    def test_renamed_column_suggests_closest_source_column(self):
        """A typo/near-rename ('customer_emial') isn't a plural/singular or
        casing variant of 'customer_email', so find_match() can't bridge it
        -- but it's close enough for a fuzzy suggestion. (Note: a pure
        abbreviation like 'ltv' for 'lifetime_value' is too dissimilar by
        edit distance for this heuristic to catch -- that case still needs an
        explicit mapping.columns entry with no assistance.)"""
        source_extractor, target_extractor = self._extractors(
            {
                "customers": Schema(
                    name="customers",
                    columns=[
                        {
                            "name": "customer_email",
                            "type": "varchar",
                            "required": True,
                        }
                    ],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )
            },
            {
                "customers": Schema(
                    name="customers",
                    columns=[
                        {"name": "customer_emial", "type": "str", "required": True}
                    ],
                    source="test",
                )
            },
        )

        result = ContractValidator(source_extractor, target_extractor).validate()

        assert result.success is False
        issue = result.critical_issues[0]
        assert issue.category == "Missing Column"
        assert "customer_email" in issue.suggested_fix
        assert "mapping.columns" in issue.suggested_fix

    def test_dissimilar_column_name_gets_no_false_suggestion(self):
        """Two unrelated names (an intentional mapping case, not a typo/rename)
        shouldn't produce a misleading guess."""
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

        result = ContractValidator(source_extractor, target_extractor).validate()

        assert result.success is False
        issue = result.critical_issues[0]
        assert "Did you mean" not in issue.suggested_fix

    def test_missing_table_suggests_closest_source_table(self):
        """A typo ('subscriptons') isn't a plural/singular or casing variant
        of 'subscriptions', so find_match() can't bridge it -- but it's close
        enough for a fuzzy suggestion."""
        source_extractor, target_extractor = self._extractors(
            {
                "subscriptions": Schema(
                    name="subscriptions",
                    columns=[{"name": "id", "type": "varchar", "required": True}],
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )
            },
            {
                "subscriptons": Schema(
                    name="subscriptons",
                    columns=[{"name": "id", "type": "str", "required": True}],
                    source="test",
                )
            },
        )

        result = ContractValidator(source_extractor, target_extractor).validate()

        assert result.success is False
        issue = result.critical_issues[0]
        assert issue.category == "Missing Table"
        assert "subscriptions" in issue.suggested_fix
        assert "mapping.tables" in issue.suggested_fix
