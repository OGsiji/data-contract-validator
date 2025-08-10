"""
Core validation logic for comparing schemas.
"""

from typing import Dict, List
from .models import ValidationResult, ValidationIssue, IssueSeverity, Schema
from ..extractors.base import BaseExtractor


class ContractValidator:
    """
    Main contract validator that compares schemas from different sources.
    """

    def __init__(
        self, source_extractor: BaseExtractor, target_extractor: BaseExtractor
    ):
        """
        Initialize validator with source and target extractors.

        Args:
            source_extractor: Extractor for source schemas (e.g., DBT)
            target_extractor: Extractor for target schemas (e.g., FastAPI)
        """
        self.source_extractor = source_extractor
        self.target_extractor = target_extractor
        self.issues: List[ValidationIssue] = []

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

        # Validate each target schema against source
        print("🔍 Validating schema compatibility...")
        for table_name, target_schema in target_schemas.items():
            self._validate_table(table_name, target_schema, source_schemas)

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
        self, table_name: str, target_schema: Schema, source_schemas: Dict[str, Schema]
    ):
        """Validate a single table."""
        print(f"  🔍 Validating table: {table_name}")

        # Check if source provides this table
        source_schema = source_schemas.get(table_name)
        if not source_schema:
            self.issues.append(
                ValidationIssue(
                    severity=IssueSeverity.CRITICAL,
                    table=table_name,
                    column=None,
                    message=f"Target expects table '{table_name}' but source doesn't provide it",
                    category="Missing Table",
                    suggested_fix=f"Create a source model that outputs table '{table_name}'",
                )
            )
            print(f"    ❌ Table '{table_name}' missing in source")
            return

        # Check columns
        source_columns = {col["name"]: col for col in source_schema.columns}
        target_columns = {col["name"]: col for col in target_schema.columns}

        # Check for missing required columns
        for col_name, col_info in target_columns.items():
            if col_name not in source_columns:
                is_required = col_info.get("required", True)
                severity = (
                    IssueSeverity.CRITICAL if is_required else IssueSeverity.WARNING
                )

                self.issues.append(
                    ValidationIssue(
                        severity=severity,
                        table=table_name,
                        column=col_name,
                        message=f"Target {'REQUIRES' if is_required else 'expects'} column '{col_name}' but source doesn't provide it",
                        category="Missing Column",
                        suggested_fix=f"Add column '{col_name}' to source model for table '{table_name}'",
                    )
                )
            else:
                # Check type compatibility
                source_col = source_columns[col_name]
                target_col = col_info

                if not self._types_compatible(
                    source_col.get("type"), target_col.get("type")
                ):
                    self.issues.append(
                        ValidationIssue(
                            severity=IssueSeverity.WARNING,
                            table=table_name,
                            column=col_name,
                            message=f"Type mismatch: source provides '{source_col.get('type')}' but target expects '{target_col.get('type')}'",
                            category="Type Mismatch",
                            source_value=source_col.get("type"),
                            target_value=target_col.get("type"),
                            suggested_fix=f"Update target model to expect '{source_col.get('type')}' or fix source column type",
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

    def _types_compatible(self, source_type: str, target_type: str) -> bool:
        """Check if source and target types are compatible."""
        if not source_type or not target_type:
            return True  # Skip validation if types are unknown

        # Normalize types
        source_type = source_type.lower()
        target_type = target_type.lower()

        # Exact match
        if source_type == target_type:
            return True

        # Compatible type mappings
        compatible_types = {
            "varchar": ["string", "str", "text"],
            "string": ["varchar", "text"],
            "text": ["varchar", "string"],
            "integer": ["int", "bigint"],
            "int": ["integer", "bigint"],
            "bigint": ["integer", "int"],
            "float": ["double", "decimal", "numeric", "real"],
            "double": ["float", "decimal"],
            "boolean": ["bool"],
            "bool": ["boolean"],
            "timestamp": ["datetime"],
            "datetime": ["timestamp"],
        }

        return target_type in compatible_types.get(source_type, [])

    def _generate_summary(self, success: bool, issues: List[ValidationIssue]) -> str:
        """Generate validation summary."""
        if success:
            return f"✅ Validation passed with {len(issues)} non-critical issues"
        else:
            critical = [i for i in issues if i.severity == IssueSeverity.CRITICAL]
            return f"❌ Validation failed with {len(critical)} critical issues"
