"""
Core validation logic for comparing schemas.
"""

from typing import Dict, List, Optional, Any
from .models import ValidationResult, ValidationIssue, IssueSeverity, Schema
from .types import CanonicalType, normalize_name, normalize_sql_type, types_compatible
from ..extractors.base import BaseExtractor


class ContractValidator:
    """
    Main contract validator that compares schemas from different sources.
    """

    def __init__(
        self,
        source_extractor: BaseExtractor,
        target_extractor: BaseExtractor,
        mapping: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize validator with source and target extractors.

        Args:
            source_extractor: Extractor for source schemas (e.g., DBT)
            target_extractor: Extractor for target schemas (e.g., FastAPI)
            mapping: Optional explicit mapping for when name heuristics aren't
                enough. Shape::

                    {
                      "tables":  {"<target_table>": "<source_table>"},
                      "columns": {"<target_table>": {"<target_col>": "<source_col>"}},
                    }

                Keys are matched case/style-insensitively (userId == user_id).
        """
        self.source_extractor = source_extractor
        self.target_extractor = target_extractor
        self.issues: List[ValidationIssue] = []

        mapping = mapping or {}
        # target table (normalized) -> source table name
        self.table_map: Dict[str, str] = {
            normalize_name(k): v for k, v in (mapping.get("tables") or {}).items()
        }
        # target table (normalized) -> {target col (normalized) -> source col name}
        self.column_map: Dict[str, Dict[str, str]] = {
            normalize_name(table): {
                normalize_name(tcol): scol for tcol, scol in cols.items()
            }
            for table, cols in (mapping.get("columns") or {}).items()
        }

    def validate(self) -> ValidationResult:
        """
        Run validation and return results.

        Returns:
            ValidationResult with success status and any issues found
        """
        print("🔍 Starting contract validation...")

        # Extract schemas
        print("📊 Extracting source schemas...")
        source_schemas = self.source_extractor.extract_schemas()

        print("🎯 Extracting target schemas...")
        target_schemas = self.target_extractor.extract_schemas()

        print(f"   Source: {len(source_schemas)} schemas")
        print(f"   Target: {len(target_schemas)} schemas")

        # Reset issues
        self.issues = []

        # Index source schemas by normalized name so userId/user_id/USERS match.
        source_by_norm = {
            normalize_name(name): schema for name, schema in source_schemas.items()
        }

        # Validate each target schema against source
        print("🔍 Validating schema compatibility...")
        for table_name, target_schema in target_schemas.items():
            self._validate_table(table_name, target_schema, source_by_norm)

        # Determine success
        critical_issues = [
            i for i in self.issues if i.severity == IssueSeverity.CRITICAL
        ]
        success = len(critical_issues) == 0

        # Generate summary
        summary = self._generate_summary(success, self.issues)

        return ValidationResult(
            success=success,
            issues=self.issues,
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            summary=summary,
        )

    def _validate_table(
        self, table_name: str, target_schema: Schema, source_by_norm: Dict[str, Schema]
    ):
        """Validate a single table."""
        print(f"  🔍 Validating table: {table_name}")

        # Resolve the source table: explicit mapping first, else normalized name.
        target_norm = normalize_name(table_name)
        mapped_source = self.table_map.get(target_norm)
        lookup_norm = normalize_name(mapped_source) if mapped_source else target_norm
        source_schema = source_by_norm.get(lookup_norm)
        if not source_schema:
            hint = f" (mapped to source '{mapped_source}')" if mapped_source else ""
            self.issues.append(
                ValidationIssue(
                    severity=IssueSeverity.CRITICAL,
                    table=table_name,
                    column=None,
                    message=(
                        f"Target expects table '{table_name}'{hint} but source "
                        f"doesn't provide it"
                    ),
                    category="Missing Table",
                    suggested_fix=(
                        f"Create a source model that outputs table "
                        f"'{mapped_source or table_name}', or add a 'mapping.tables' "
                        f"entry pointing '{table_name}' at the right source model"
                    ),
                )
            )
            print(f"    ❌ Table '{table_name}' missing in source")
            return

        # Index columns by normalized name so casing/snake/camel differences match.
        source_columns = {
            normalize_name(col["name"]): col for col in source_schema.columns
        }
        target_columns = {
            normalize_name(col["name"]): col for col in target_schema.columns
        }

        # Per-table explicit column overrides: target col -> source col.
        col_overrides = self.column_map.get(target_norm, {})

        # If we couldn't fully see the source columns (e.g. SELECT *), a missing
        # column is unprovable -- never hard-fail on it.
        source_complete = source_schema.is_complete
        # Regex-tier types are unreliable; don't raise type warnings off them.
        check_types = source_schema.confidence != "low"

        for col_norm, col_info in target_columns.items():
            # Apply an explicit column mapping for this target column, if any.
            override = col_overrides.get(col_norm)
            source_key = normalize_name(override) if override else col_norm

            if source_key not in source_columns:
                is_required = col_info.get("required", True)
                if is_required and source_complete:
                    severity = IssueSeverity.CRITICAL
                else:
                    severity = IssueSeverity.WARNING

                qualifier = ""
                if is_required and not source_complete:
                    qualifier = (
                        " (source columns could not be fully resolved, e.g. "
                        "SELECT * — verify manually)"
                    )

                self.issues.append(
                    ValidationIssue(
                        severity=severity,
                        table=table_name,
                        column=col_info.get("name"),
                        message=(
                            f"Target {'REQUIRES' if is_required else 'expects'} column "
                            f"'{col_info.get('name')}' but source doesn't provide it"
                            f"{qualifier}"
                        ),
                        category="Missing Column",
                        suggested_fix=(
                            f"Add column '{col_info.get('name')}' to source model "
                            f"for table '{table_name}'"
                        ),
                    )
                )
            elif check_types:
                source_col = source_columns[source_key]
                if not self._columns_type_compatible(source_col, col_info):
                    self.issues.append(
                        ValidationIssue(
                            severity=IssueSeverity.WARNING,
                            table=table_name,
                            column=col_info.get("name"),
                            message=(
                                f"Type mismatch: source provides "
                                f"'{source_col.get('type')}' but target expects "
                                f"'{col_info.get('type')}'"
                            ),
                            category="Type Mismatch",
                            source_value=source_col.get("type"),
                            target_value=col_info.get("type"),
                            suggested_fix=(
                                f"Update target model to accept "
                                f"'{source_col.get('type')}' or fix source column type"
                            ),
                        )
                    )

        # Log results for this table
        table_issues = [i for i in self.issues if i.table == table_name]
        if not table_issues:
            print(f"    ✅ All requirements satisfied")
        else:
            critical = [i for i in table_issues if i.severity == IssueSeverity.CRITICAL]
            warnings = [i for i in table_issues if i.severity == IssueSeverity.WARNING]
            if critical:
                print(f"    🚨 {len(critical)} critical issues")
            if warnings:
                print(f"    ⚠️  {len(warnings)} warnings")

    def _columns_type_compatible(self, source_col: dict, target_col: dict) -> bool:
        """Compare two columns using their canonical types."""
        source_type = self._canonical_type(source_col)
        target_type = self._canonical_type(target_col)
        return types_compatible(source_type, target_type)

    @staticmethod
    def _canonical_type(col: dict) -> CanonicalType:
        """Resolve a column's canonical type, falling back to SQL normalization."""
        raw_canon = col.get("canonical_type")
        if raw_canon:
            try:
                return CanonicalType(raw_canon)
            except ValueError:
                pass
        return normalize_sql_type(col.get("type"))

    def _generate_summary(self, success: bool, issues: List[ValidationIssue]) -> str:
        """Generate validation summary."""
        if success:
            return f"✅ Validation passed with {len(issues)} non-critical issues"
        else:
            critical = [i for i in issues if i.severity == IssueSeverity.CRITICAL]
            return f"❌ Validation failed with {len(critical)} critical issues"
