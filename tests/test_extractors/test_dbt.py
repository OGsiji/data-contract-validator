"""
Tests for the tiered DBT extractor (sqlglot + catalog.json).
"""

import json
import pytest

from data_contract_validator.extractors.dbt import DBTExtractor, _HAS_SQLGLOT
from data_contract_validator.core.types import CanonicalType


def _write_model(project_dir, name, sql):
    models = project_dir / "models"
    models.mkdir(exist_ok=True)
    (models / f"{name}.sql").write_text(sql)


@pytest.fixture
def dbt_project(temp_dir):
    project = temp_dir / "dbt_project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: test\nversion: '1.0'\n")
    return project


@pytest.mark.skipif(not _HAS_SQLGLOT, reason="sqlglot not installed")
class TestSqlglotParsing:
    def test_handles_ctes_and_uses_final_select(self, dbt_project):
        # A CTE with different columns than the final SELECT. The regex parser
        # would grab the wrong (last-before-FROM) projection; sqlglot must use
        # the outer projection.
        sql = """
        with base as (
            select id, raw_email, signup_ts from {{ ref('raw_users') }}
        )
        select
            id as user_id,
            lower(raw_email) as email,
            signup_ts as created_at
        from base
        """
        _write_model(dbt_project, "users", sql)

        schemas = DBTExtractor(str(dbt_project)).extract_schemas()

        assert "users" in schemas
        names = {c["name"] for c in schemas["users"].columns}
        assert names == {"user_id", "email", "created_at"}
        assert schemas["users"].is_complete
        assert schemas["users"].confidence == "medium"

    def test_select_star_marked_incomplete(self, dbt_project):
        sql = "select * from {{ ref('raw_users') }}"
        _write_model(dbt_project, "all_users", sql)

        schemas = DBTExtractor(str(dbt_project)).extract_schemas()

        # Either no columns or flagged incomplete -- the key property is that it
        # is NOT treated as a complete, authoritative column set.
        assert not schemas.get("all_users", _Complete()).is_complete

    def test_cast_infers_canonical_type(self, dbt_project):
        sql = "select cast(amount as integer) as total from {{ ref('orders') }}"
        _write_model(dbt_project, "orders_summary", sql)

        schemas = DBTExtractor(str(dbt_project)).extract_schemas()
        col = schemas["orders_summary"].columns[0]
        assert col["canonical_type"] == CanonicalType.INTEGER.value


class _Complete:
    """Sentinel so .get(...) default has an is_complete attribute."""

    is_complete = True


class TestCatalogExtraction:
    def test_catalog_gives_real_types_and_high_confidence(self, dbt_project):
        target = dbt_project / "target"
        target.mkdir()
        catalog = {
            "nodes": {
                "model.test.users": {
                    "metadata": {"name": "users"},
                    "columns": {
                        "USER_ID": {"type": "NUMBER(38,0)"},
                        "EMAIL": {"type": "VARCHAR(255)"},
                        "CREATED_AT": {"type": "TIMESTAMP_NTZ"},
                    },
                }
            }
        }
        (target / "catalog.json").write_text(json.dumps(catalog))
        # Also a SQL file that would parse differently -- catalog must win.
        _write_model(dbt_project, "users", "select 1")

        extractor = DBTExtractor(str(dbt_project))
        # Avoid invoking the real dbt CLI during the test.
        extractor._try_compile_dbt = lambda: False
        schemas = extractor.extract_schemas()

        assert "users" in schemas
        users = schemas["users"]
        assert users.confidence == "high"
        assert users.is_complete
        types = {c["name"]: c["canonical_type"] for c in users.columns}
        assert types["user_id"] == CanonicalType.BIGINT.value
        assert types["email"] == CanonicalType.STRING.value
        assert types["created_at"] == CanonicalType.TIMESTAMP.value
