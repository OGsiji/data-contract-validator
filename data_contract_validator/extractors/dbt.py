# data_contract_validator/extractors/dbt.py
"""
DBT schema extractor.

Extraction uses a tiered strategy, preferring the most trustworthy source of
truth available and degrading gracefully:

    Tier 1  catalog.json   -> real warehouse column types (dbt docs generate).
                              High confidence, complete column set.
    Tier 2  sqlglot parse  -> a proper SQL parser. Column *names* are trusted;
                              types are inferred (often unknown) and may be
                              enriched from manifest.json. ``SELECT *`` is
                              detected and the schema flagged incomplete so it
                              never triggers a false "missing column".
    Tier 3  regex parse    -> last-resort best effort. Low confidence; never
                              used to hard-fail a build.

The confidence/completeness of each tier is recorded on the Schema so the
validator can avoid raising critical issues on data it isn't sure about.
"""

import json
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from .base import BaseExtractor
from ..core.models import Schema
from ..core.types import CanonicalType, normalize_sql_type

try:  # sqlglot is optional; we degrade to regex parsing when it is absent.
    import sqlglot
    from sqlglot import exp

    _HAS_SQLGLOT = True
except ImportError:  # pragma: no cover - exercised only without the dependency
    _HAS_SQLGLOT = False


class DBTExtractor(BaseExtractor):
    """Extract schemas from DBT projects."""

    def __init__(
        self,
        project_path: str = ".",
        disable_manifest: bool = False,
        dialect: Optional[str] = None,
    ):
        self.project_path = Path(project_path)
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self.catalog_path = self.target_dir / "catalog.json"
        self.models_path = self.project_path / "models"
        self.disable_manifest = disable_manifest
        self.dialect = dialect

    def extract_schemas(self) -> Dict[str, Schema]:
        """Extract schemas from DBT project using the tiered strategy."""
        print(f"🔍 Extracting DBT schemas from {self.project_path}")

        if self.disable_manifest:
            print("   📄 Manifest/catalog disabled, using SQL file parsing")
            return self._extract_from_sql_files()

        # Best-effort compile so manifest.json exists for type enrichment.
        self._try_compile_dbt()

        # Tier 1: catalog.json carries real warehouse types.
        if self.catalog_path.exists():
            print("   📚 Using catalog.json (real warehouse types)")
            schemas = self._extract_from_catalog()
            if schemas:
                return schemas
            print("   ⚠️  catalog.json yielded no models, falling back")

        # Tier 2/3: parse the SQL (sqlglot, then regex), enriched with manifest.
        print("   📄 Using SQL parsing" + ("" if _HAS_SQLGLOT else " (regex only)"))
        return self._extract_from_sql_files()

    def _try_compile_dbt(self) -> bool:
        """Try to parse the DBT project so manifest.json is fresh."""
        try:
            result = subprocess.run(
                ["dbt", "parse", "--project-dir", str(self.project_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            print("   ⚠️  DBT compilation timeout (>60s)")
            return False
        except FileNotFoundError:
            print("   ⚠️  DBT CLI not found (install with: pip install dbt-core)")
            return False
        except Exception as e:
            print(f"   ⚠️  DBT compilation error: {e}")
            return False

    # -- Tier 1: catalog.json -------------------------------------------------

    def _extract_from_catalog(self) -> Dict[str, Schema]:
        """Extract schemas from catalog.json (authoritative warehouse types)."""
        try:
            with open(self.catalog_path, "r") as f:
                catalog = json.load(f)
        except Exception as e:
            print(f"   ❌ Could not read catalog.json: {e}")
            return {}

        schemas: Dict[str, Schema] = {}
        for unique_id, node in catalog.get("nodes", {}).items():
            if not unique_id.startswith("model."):
                continue

            model_name = unique_id.split(".")[-1]
            columns = []
            for col_name, col_info in node.get("columns", {}).items():
                raw_type = col_info.get("type")
                columns.append(
                    self._make_column(
                        col_name.lower(),
                        raw_type=raw_type,
                        canonical_type=normalize_sql_type(raw_type),
                    )
                )

            if columns:
                schemas[model_name] = Schema(
                    name=model_name,
                    columns=columns,
                    source="dbt_catalog",
                    metadata={"confidence": "high", "complete": True},
                )

        print(f"   ✅ Found {len(schemas)} models in catalog.json")
        return schemas

    # -- manifest type enrichment --------------------------------------------

    def _load_manifest_types(self) -> Dict[str, Dict[str, str]]:
        """Map model name -> {column -> documented data_type} from manifest.json."""
        if not self.manifest_path.exists():
            return {}
        try:
            with open(self.manifest_path, "r") as f:
                manifest = json.load(f)
        except Exception:
            return {}

        out: Dict[str, Dict[str, str]] = {}
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") != "model":
                continue
            model_name = node.get("alias") or node.get("name")
            cols = {}
            for col_name, col_info in node.get("columns", {}).items():
                data_type = col_info.get("data_type")
                if data_type:
                    cols[col_name.lower()] = data_type
            if cols:
                out[model_name] = cols
        return out

    # -- Tier 2/3: SQL file parsing ------------------------------------------

    def _extract_from_sql_files(self) -> Dict[str, Schema]:
        """Extract schemas from SQL files (sqlglot first, regex fallback)."""
        schemas: Dict[str, Schema] = {}

        if not self.models_path.exists():
            print(f"   ⚠️  Models directory not found: {self.models_path}")
            return schemas

        sql_files = list(self.models_path.rglob("*.sql"))
        print(f"   🔍 Found {len(sql_files)} SQL files to analyze")

        manifest_types = self._load_manifest_types()

        for sql_file in sql_files:
            model_name = sql_file.stem

            # Skip test/analysis/macro files
            if any(skip in str(sql_file) for skip in ["tests", "analysis", "macros"]):
                continue

            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    sql_content = f.read()
            except Exception as e:
                print(f"   ❌ Error reading {model_name}: {e}")
                continue

            columns, complete, confidence = self._extract_columns(sql_content)

            # Skip only when we're confident there is genuinely nothing here.
            # If the column set is merely incomplete (e.g. SELECT *), we still
            # emit the model so the validator knows the table EXISTS -- otherwise
            # it would wrongly report a "missing table".
            if not columns and complete:
                continue

            # Enrich unknown types with documented manifest types when available.
            self._enrich_types(columns, manifest_types.get(model_name, {}))

            schemas[model_name] = Schema(
                name=model_name,
                columns=columns,
                source="dbt_sqlglot" if confidence == "medium" else "dbt_regex",
                metadata={"confidence": confidence, "complete": complete},
            )
            flag = "" if complete else " (incomplete: SELECT * / unresolved)"
            print(f"   📋 {model_name}: {len(columns)} columns [{confidence}]{flag}")

        return schemas

    def _enrich_types(
        self, columns: List[Dict[str, Any]], documented: Dict[str, str]
    ) -> None:
        """Fill in canonical types from documented manifest types where unknown."""
        if not documented:
            return
        for col in columns:
            if col.get("canonical_type") != CanonicalType.UNKNOWN.value:
                continue
            data_type = documented.get(col["name"])
            if data_type:
                col["type"] = data_type
                col["canonical_type"] = normalize_sql_type(data_type).value

    def _extract_columns(
        self, sql_content: str
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """Return (columns, complete, confidence) for a model's SQL."""
        if _HAS_SQLGLOT:
            result = self._extract_columns_sqlglot(sql_content)
            if result is not None:
                columns, complete = result
                return columns, complete, "medium"

        # Tier 3: regex best-effort.
        columns = self._extract_columns_regex(sql_content)
        # Regex output is never trustworthy enough to claim completeness.
        return columns, False, "low"

    # -- sqlglot parsing ------------------------------------------------------

    def _strip_jinja(self, sql: str) -> str:
        """Replace dbt Jinja so sqlglot can parse the SQL.

        ``{{ ref('x') }}`` / ``{{ source('a','b') }}`` become bare table names;
        other expressions become a harmless placeholder; control/comment blocks
        are removed.
        """

        def repl_expr(match: "re.Match") -> str:
            inner = match.group(1)
            ref = re.search(r"ref\(\s*['\"]([^'\"]+)['\"]", inner)
            if ref:
                return ref.group(1)
            src = re.search(
                r"source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]", inner
            )
            if src:
                return src.group(1)
            return "jinja_placeholder"

        sql = re.sub(r"\{\{(.*?)\}\}", repl_expr, sql, flags=re.DOTALL)
        sql = re.sub(r"\{%.*?%\}", " ", sql, flags=re.DOTALL)
        sql = re.sub(r"\{#.*?#\}", " ", sql, flags=re.DOTALL)
        return sql

    def _extract_columns_sqlglot(
        self, sql_content: str
    ) -> Optional[Tuple[List[Dict[str, Any]], bool]]:
        """Parse columns with sqlglot. Returns None to signal a parse failure."""
        cleaned = self._strip_jinja(sql_content)
        try:
            parsed = sqlglot.parse(cleaned, read=self.dialect)
        except Exception:
            return None

        statements = [s for s in parsed if s is not None]
        if not statements:
            return None

        expr = statements[-1]
        select = self._final_select(expr)
        if select is None:
            return None

        return self._columns_from_select(select)

    def _final_select(self, expr: "exp.Expression") -> "Optional[exp.Select]":
        """Resolve the outermost projection of a (possibly UNION/CTE) statement."""
        if isinstance(expr, exp.Select):
            return expr
        if isinstance(expr, exp.Union):
            left = expr.this
            while isinstance(left, exp.Union):
                left = left.this
            return left if isinstance(left, exp.Select) else None
        # e.g. a wrapping subquery
        inner = expr.find(exp.Select)
        return inner

    def _columns_from_select(
        self, select: "exp.Select"
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Extract output columns from a SELECT; flag completeness on star/unnamed."""
        columns: List[Dict[str, Any]] = []
        complete = True

        for proj in select.expressions:
            if proj.is_star:  # SELECT * or table.*
                complete = False
                continue

            name = proj.alias_or_name
            if not name:
                # An unnamed complex expression -> we can't trust the column set.
                complete = False
                continue

            canon = self._infer_canonical_from_expr(proj)
            columns.append(
                self._make_column(
                    name.lower(),
                    raw_type=None if canon == CanonicalType.UNKNOWN else canon.value,
                    canonical_type=canon,
                )
            )

        return columns, complete

    def _infer_canonical_from_expr(self, proj: "exp.Expression") -> CanonicalType:
        """Light type inference for a projection (most resolve to UNKNOWN)."""
        node = proj.this if isinstance(proj, exp.Alias) else proj

        if isinstance(node, exp.Cast):
            return normalize_sql_type(node.to.sql())
        if isinstance(node, exp.Count):
            return CanonicalType.BIGINT
        if isinstance(node, exp.Boolean):
            return CanonicalType.BOOLEAN
        if isinstance(node, exp.Literal):
            return CanonicalType.STRING if node.is_string else CanonicalType.UNKNOWN
        return CanonicalType.UNKNOWN

    # -- Tier 3: regex parsing (legacy best-effort) --------------------------

    def _extract_columns_regex(self, sql_content: str) -> List[Dict[str, Any]]:
        """Extract columns from SQL content via regex - simplified best effort."""
        cleaned = re.sub(r"--.*?\n", "\n", sql_content)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"\{\{.*?\}\}", "", cleaned)

        select_matches = list(
            re.finditer(r"select\s+(.*?)\s+from", cleaned, re.DOTALL | re.IGNORECASE)
        )
        if not select_matches:
            return []

        select_content = select_matches[-1].group(1).strip()

        columns: List[Dict[str, Any]] = []
        for col_text in self._split_columns(select_content):
            col_text = col_text.strip()
            if col_text and col_text != "*":
                column_name = self._extract_column_name(col_text)
                if column_name:
                    canon = self._infer_data_type_regex(col_text)
                    columns.append(
                        self._make_column(
                            column_name,
                            raw_type=(
                                None if canon == CanonicalType.UNKNOWN else canon.value
                            ),
                            canonical_type=canon,
                        )
                    )

        return columns

    def _split_columns(self, select_clause: str) -> List[str]:
        """Split SELECT columns by comma, handling nested functions."""
        columns = []
        current_column = ""
        paren_depth = 0

        for char in select_clause:
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            elif char == "," and paren_depth == 0:
                if current_column.strip():
                    columns.append(current_column.strip())
                current_column = ""
                continue

            current_column += char

        if current_column.strip():
            columns.append(current_column.strip())

        return columns

    def _extract_column_name(self, col_text: str) -> Optional[str]:
        """Extract clean column name from column definition."""
        col_text = col_text.strip()

        as_match = re.search(r"\s+as\s+(\w+)$", col_text, re.IGNORECASE)
        if as_match:
            return as_match.group(1).lower()

        table_match = re.search(r"(\w+)\.(\w+)$", col_text)
        if table_match:
            return table_match.group(2).lower()

        simple_match = re.search(r"^(\w+)$", col_text)
        if simple_match:
            return simple_match.group(1).lower()

        parts = col_text.split()
        if len(parts) > 1 and not "(" in parts[-1]:
            return parts[-1].lower()

        return None

    def _infer_data_type_regex(self, expression: str) -> CanonicalType:
        """Infer a canonical type from a SQL expression (regex tier)."""
        expr_upper = expression.upper()

        if any(func in expr_upper for func in ["COUNT", "SUM", "ROW_NUMBER"]):
            return CanonicalType.BIGINT
        elif "AVG" in expr_upper:
            return CanonicalType.FLOAT
        elif any(func in expr_upper for func in ["CONCAT", "UPPER", "LOWER"]):
            return CanonicalType.STRING
        elif "TIMESTAMP" in expr_upper or "CURRENT_TIMESTAMP" in expr_upper:
            return CanonicalType.TIMESTAMP
        elif "DATE" in expr_upper:
            return CanonicalType.DATE
        elif any(keyword in expr_upper for keyword in ["TRUE", "FALSE", "BOOLEAN"]):
            return CanonicalType.BOOLEAN
        else:
            return CanonicalType.UNKNOWN
