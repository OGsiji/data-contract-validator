"""
Tests for the canonical type system.
"""

from data_contract_validator.core.types import (
    CanonicalType,
    normalize_sql_type,
    normalize_python_type,
    normalize_name,
    name_variants,
    find_match,
    types_compatible,
)


class TestNormalizeSqlType:
    def test_common_strings(self):
        for raw in ["varchar", "VARCHAR(255)", "text", "STRING", "char(1)"]:
            assert normalize_sql_type(raw) == CanonicalType.STRING

    def test_integers_and_bigints(self):
        assert normalize_sql_type("integer") == CanonicalType.INTEGER
        assert normalize_sql_type("INT64") == CanonicalType.BIGINT  # BigQuery
        assert normalize_sql_type("bigint") == CanonicalType.BIGINT

    def test_snowflake_number_scale_zero_is_integer(self):
        # NUMBER(p, 0) is really an integer in Snowflake/Oracle.
        assert normalize_sql_type("NUMBER(9,0)") == CanonicalType.INTEGER
        assert normalize_sql_type("NUMBER(38,0)") == CanonicalType.BIGINT
        # With a scale it is a decimal.
        assert normalize_sql_type("NUMBER(10,2)") == CanonicalType.DECIMAL

    def test_timestamps(self):
        for raw in ["timestamp", "datetime", "TIMESTAMP_NTZ", "timestamptz"]:
            assert normalize_sql_type(raw) == CanonicalType.TIMESTAMP

    def test_unknown_is_unknown(self):
        assert normalize_sql_type("some_custom_udt") == CanonicalType.UNKNOWN
        assert normalize_sql_type(None) == CanonicalType.UNKNOWN
        assert normalize_sql_type("") == CanonicalType.UNKNOWN


class TestNormalizePythonType:
    def test_builtins(self):
        assert normalize_python_type("str") == CanonicalType.STRING
        assert normalize_python_type("int") == CanonicalType.INTEGER
        assert normalize_python_type("bool") == CanonicalType.BOOLEAN

    def test_optional_unwrapped(self):
        assert normalize_python_type("Optional[int]") == CanonicalType.INTEGER
        assert normalize_python_type("Optional[str]") == CanonicalType.STRING

    def test_containers(self):
        assert normalize_python_type("List[Item]") == CanonicalType.ARRAY
        assert normalize_python_type("Dict[str, Any]") == CanonicalType.JSON

    def test_qualified_names(self):
        assert normalize_python_type("datetime.datetime") == CanonicalType.TIMESTAMP
        assert normalize_python_type("uuid.UUID") == CanonicalType.UUID

    def test_custom_class_is_unknown(self):
        assert normalize_python_type("SomeNestedModel") == CanonicalType.UNKNOWN


class TestTypesCompatible:
    def test_varchar_matches_str(self):
        # The classic false positive this whole change exists to kill.
        assert types_compatible(
            normalize_sql_type("varchar"), normalize_python_type("str")
        )

    def test_timestamp_matches_datetime(self):
        assert types_compatible(
            normalize_sql_type("timestamp"), normalize_python_type("datetime.datetime")
        )

    def test_numeric_widening(self):
        assert types_compatible(CanonicalType.INTEGER, CanonicalType.BIGINT)
        assert types_compatible(CanonicalType.INTEGER, CanonicalType.FLOAT)
        # Narrowing is not safe.
        assert not types_compatible(CanonicalType.FLOAT, CanonicalType.INTEGER)

    def test_unknown_is_always_compatible(self):
        assert types_compatible(CanonicalType.UNKNOWN, CanonicalType.INTEGER)
        assert types_compatible(CanonicalType.STRING, CanonicalType.UNKNOWN)

    def test_genuine_mismatch_flagged(self):
        # string vs boolean is a real mismatch and must NOT be compatible.
        assert not types_compatible(CanonicalType.STRING, CanonicalType.BOOLEAN)


class TestNormalizeName:
    def test_camel_and_snake_fold(self):
        assert normalize_name("userId") == normalize_name("user_id")
        assert normalize_name("USER_ID") == "user_id"
        assert normalize_name("UserProfile") == "user_profile"


class TestPluralSingularMatching:
    def test_variants_bridge_plural_and_singular(self):
        # The plural form should be among the singular name's candidates...
        assert "users" in name_variants("User")
        assert "categories" in name_variants("category")
        assert "addresses" in name_variants("address")
        # ...and vice versa.
        assert "user" in name_variants("users")
        assert "category" in name_variants("categories")
        assert "address" in name_variants("addresses")

    def test_does_not_overstrip_words_ending_in_double_s(self):
        # 'address' must never collapse to 'addres'.
        assert "addres" not in name_variants("address")

    def test_find_match_exact_wins_over_variant(self):
        index = {"user": "SINGULAR", "users": "PLURAL"}
        assert find_match("users", index) == "PLURAL"
        assert find_match("user", index) == "SINGULAR"

    def test_find_match_falls_back_to_variant(self):
        index = {"users": "the_users_model"}
        assert find_match("User", index) == "the_users_model"
        assert find_match("missing", index) is None
