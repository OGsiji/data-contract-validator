#!/usr/bin/env python3
"""
YAML-Based Contract Validator
Validates compatibility between FastAPI and DBT schemas using YAML files directly
"""

import yaml
import sys
import os
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from .models import ValidationIssue, ValidationSeverity


class YAMLContractValidator:
    def __init__(self, fastapi_yaml_path: str, dbt_yaml_path: str):
        """
        Initialize validator with YAML file paths

        Args:
            fastapi_yaml_path: Path to FastAPI contract YAML file
            dbt_yaml_path: Path to DBT contract YAML file
        """
        self.fastapi_yaml_path = Path(fastapi_yaml_path)
        self.dbt_yaml_path = Path(dbt_yaml_path)
        self.issues: List[ValidationIssue] = []

    def validate_contracts(self) -> Tuple[bool, List[ValidationIssue]]:
        """
        Main validation method - focuses on FastAPI requirements that DBT doesn't meet

        Returns:
            Tuple of (success: bool, issues: List[ValidationIssue])
        """
        print("🔍 Starting YAML contract validation...")
        print("   Focus: FastAPI requirements that DBT doesn't provide")

        try:
            # Step 1: Load FastAPI schema requirements
            print(f"📋 Loading FastAPI requirements from {self.fastapi_yaml_path}...")
            fastapi_schemas = self._load_fastapi_schema()

            if not fastapi_schemas:
                self.issues.append(
                    ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        category="FastAPI",
                        table="N/A",
                        column=None,
                        message="No FastAPI tables found in YAML - nothing to validate",
                    )
                )
                return True, self.issues

            print(f"   Found {len(fastapi_schemas)} FastAPI models requiring data")

            # Step 2: Load DBT schema outputs
            print(f"📊 Loading DBT outputs from {self.dbt_yaml_path}...")
            dbt_schemas = self._load_dbt_schema()
            print(f"   Found {len(dbt_schemas)} DBT models")

            # Step 3: Focus on critical compatibility issues
            print("🔍 Checking if DBT provides what FastAPI needs...")
            self._validate_schema_compatibility(fastapi_schemas, dbt_schemas)
            self._validate_column_compatibility(fastapi_schemas, dbt_schemas)

            # Count critical issues
            critical_issues = [
                i for i in self.issues if i.severity == ValidationSeverity.ERROR
            ]

            if critical_issues:
                print(f"   🚨 Found {len(critical_issues)} CRITICAL issues!")
            else:
                print(f"   ✅ All FastAPI requirements are met!")

            return len(critical_issues) == 0, self.issues

        except Exception as e:
            self.issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="Validation",
                    table="N/A",
                    column=None,
                    message=f"Validation failed with exception: {str(e)}",
                )
            )
            return False, self.issues

    def _load_fastapi_schema(self) -> Dict[str, Any]:
        """Load FastAPI schema from YAML file"""

        if not self.fastapi_yaml_path.exists():
            raise FileNotFoundError(
                f"FastAPI YAML file not found: {self.fastapi_yaml_path}"
            )

        with open(self.fastapi_yaml_path, "r") as f:
            fastapi_contract = yaml.safe_load(f)

        # Convert to internal format
        fastapi_schemas = {}
        for table_name, table_info in fastapi_contract.get("tables", {}).items():

            # Group columns by required/optional
            required_columns = []
            optional_columns = []
            all_columns = {}

            for column in table_info.get("columns", []):
                col_name = column["name"]
                all_columns[col_name] = {
                    "name": col_name,
                    "sql_type": column.get("type", "varchar"),
                    "required": column.get("required", False),
                    "description": column.get("description", ""),
                }

                if column.get("required", False):
                    required_columns.append(col_name)
                else:
                    optional_columns.append(col_name)

            fastapi_schemas[table_name] = {
                "table_name": table_name,
                "required_columns": required_columns,
                "optional_columns": optional_columns,
                "columns": all_columns,
                "source_model": table_info.get("source_model", "Unknown"),
            }

        return fastapi_schemas

    def _load_dbt_schema(self) -> Dict[str, Any]:
        """Load DBT schema from YAML file"""

        if not self.dbt_yaml_path.exists():
            raise FileNotFoundError(f"DBT YAML file not found: {self.dbt_yaml_path}")

        with open(self.dbt_yaml_path, "r") as f:
            dbt_contract = yaml.safe_load(f)

        # Convert to internal format
        dbt_schemas = {}
        for table_name, table_info in dbt_contract.get("tables", {}).items():

            # Convert columns to dictionary format
            columns = {}
            for column in table_info.get("columns", []):
                col_name = column["name"]
                columns[col_name] = {
                    "name": col_name,
                    "data_type": column.get("type", "unknown"),
                    "description": column.get("description", ""),
                }

            dbt_schemas[table_name] = {
                "name": table_info.get("source_model", table_name),
                "alias": table_name,
                "materialization": table_info.get("materialization", "unknown"),
                "columns": columns,
                "source_model": table_info.get("source_model", "Unknown"),
            }

        return dbt_schemas

    def _validate_schema_compatibility(
        self, fastapi_schemas: Dict[str, Any], dbt_schemas: Dict[str, Any]
    ):
        """Validate overall schema compatibility - focus on missing FastAPI requirements"""

        fastapi_table_names = set(fastapi_schemas.keys())
        dbt_table_names = set(dbt_schemas.keys())

        # Find tables FastAPI NEEDS but DBT doesn't provide (CRITICAL!)
        missing_in_dbt = fastapi_table_names - dbt_table_names

        # This is what we care about most!
        for table in missing_in_dbt:
            # Check for similar names (potential typos)
            similar_tables = self._find_similar_table_names(table, dbt_table_names)
            suggestion = (
                f"Did you mean: {', '.join(similar_tables)}?"
                if similar_tables
                else "Create this table in your DBT models"
            )

            # Count how many models depend on this missing table
            dependent_models = []
            for model_name, schema in fastapi_schemas.items():
                if schema["table_name"] == table:
                    dependent_models.append(model_name)

            self.issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="🚨 CRITICAL: Missing Required Table",
                    table=table,
                    column=None,
                    message=f"FastAPI model(s) {dependent_models} REQUIRE table '{table}' but DBT doesn't provide it - API will break!",
                    fastapi_expectation=f"Table '{table}' with required columns",
                    dbt_output="❌ TABLE NOT FOUND",
                    suggestion=suggestion,
                )
            )

            print(
                f"   🚨 CRITICAL: Missing table '{table}' needed by FastAPI models: {dependent_models}"
            )

        # Extra DBT tables are not critical for now
        extra_in_dbt = dbt_table_names - fastapi_table_names
        if extra_in_dbt:
            print(
                f"   ℹ️  DBT has {len(extra_in_dbt)} extra tables not used by FastAPI: {list(extra_in_dbt)[:3]}..."
            )

    def _validate_column_compatibility(
        self, fastapi_schemas: Dict[str, Any], dbt_schemas: Dict[str, Any]
    ):
        """Validate column-level compatibility between FastAPI and DBT"""

        for table_name, fastapi_schema in fastapi_schemas.items():

            # Find corresponding DBT model
            dbt_schema = dbt_schemas.get(table_name)
            if not dbt_schema:
                continue  # Already reported as missing table

            # Check each REQUIRED column (this is what matters most!)
            fastapi_columns = fastapi_schema["columns"]
            dbt_columns = dbt_schema.get("columns", {})

            # Count missing required columns
            missing_required = []
            missing_optional = []

            for col_name, col_info in fastapi_columns.items():
                if col_name not in dbt_columns:
                    if col_info["required"]:
                        missing_required.append(col_name)
                        # This is CRITICAL - FastAPI will break
                        similar_cols = self._find_similar_column_names(
                            col_name, dbt_columns.keys()
                        )
                        suggestion = (
                            f"Did you mean: {', '.join(similar_cols)}?"
                            if similar_cols
                            else "Add this column to your DBT model"
                        )

                        self.issues.append(
                            ValidationIssue(
                                severity=ValidationSeverity.ERROR,
                                category="🚨 CRITICAL: Missing Required Column",
                                table=table_name,
                                column=col_name,
                                message=f"FastAPI REQUIRES column '{col_name}' but DBT model doesn't provide it - API will break!",
                                fastapi_expectation=f"{col_name} ({col_info['sql_type']}) - REQUIRED",
                                dbt_output="❌ MISSING",
                                suggestion=suggestion,
                            )
                        )
                    else:
                        missing_optional.append(col_name)
                        # Optional columns are less critical
                        self.issues.append(
                            ValidationIssue(
                                severity=ValidationSeverity.WARNING,
                                category="Missing Optional Column",
                                table=table_name,
                                column=col_name,
                                message=f"FastAPI expects optional column '{col_name}' but DBT model doesn't provide it",
                                fastapi_expectation=f"{col_name} ({col_info['sql_type']}) - optional",
                                dbt_output="Missing",
                                suggestion="Consider adding this column if needed",
                            )
                        )

            # Summary for this table
            if missing_required:
                print(
                    f"   🚨 CRITICAL: Table '{table_name}' missing {len(missing_required)} REQUIRED columns: {missing_required}"
                )

            # Skip type checking and extra column warnings for now (as requested)
            # We can add these back later if needed

    # Note: Type validation methods removed per user request
    # Focus is on missing tables/columns, not data type compatibility

    def _find_similar_table_names(self, target: str, candidates: Set[str]) -> List[str]:
        """Find similar table names (for typo detection)"""

        similar = []
        target_lower = target.lower()

        for candidate in candidates:
            candidate_lower = candidate.lower()

            # Simple similarity checks
            if abs(len(target_lower) - len(candidate_lower)) <= 2:
                # Check for common prefixes/suffixes
                if (
                    target_lower.startswith(candidate_lower[:3])
                    or candidate_lower.startswith(target_lower[:3])
                    or target_lower.endswith(candidate_lower[-3:])
                    or candidate_lower.endswith(target_lower[-3:])
                ):
                    similar.append(candidate)

        return similar[:3]  # Return top 3 matches

    def _find_similar_column_names(
        self, target: str, candidates: Set[str]
    ) -> List[str]:
        """Find similar column names (for typo detection)"""

        similar = []
        target_lower = target.lower()

        for candidate in candidates:
            candidate_lower = candidate.lower()

            # Simple similarity checks
            if abs(len(target_lower) - len(candidate_lower)) <= 2:
                if target_lower.startswith(
                    candidate_lower[:2]
                ) or candidate_lower.startswith(target_lower[:2]):
                    similar.append(candidate)

        return similar[:3]

    def generate_report(self, include_suggestions: bool = True) -> str:
        """Generate a comprehensive validation report focused on critical FastAPI requirements"""

        # Categorize issues by importance
        critical_errors = [
            i for i in self.issues if i.severity == ValidationSeverity.ERROR
        ]
        minor_warnings = [
            i for i in self.issues if i.severity == ValidationSeverity.WARNING
        ]
        info_items = [i for i in self.issues if i.severity == ValidationSeverity.INFO]

        report = [
            "🔍 FastAPI-DBT Contract Validation Report",
            "=" * 60,
            "",
        ]

        # Focus on what matters most!
        if critical_errors:
            report.extend(
                [
                    "🚨 CRITICAL ISSUES - API WILL BREAK! 🚨",
                    "=" * 40,
                    "These issues will cause runtime failures in your FastAPI:",
                    "",
                ]
            )

            for issue in critical_errors:
                report.append(f"💥 {issue.table}: {issue.message}")
                if issue.fastapi_expectation and issue.dbt_output:
                    report.append(f"   ✅ FastAPI needs: {issue.fastapi_expectation}")
                    report.append(f"   ❌ DBT provides: {issue.dbt_output}")
                if include_suggestions and issue.suggestion:
                    report.append(f"   🔧 FIX: {issue.suggestion}")
                report.append("")

            report.extend(["⚠️  YOU MUST FIX THESE BEFORE DEPLOYING! ⚠️", ""])

        # Summary at the top
        report.insert(-1, f"📊 SUMMARY: {len(critical_errors)} critical issues found")
        report.insert(-1, "")

        # Optional: Show less critical issues (but de-emphasized)
        if (
            minor_warnings and len(critical_errors) == 0
        ):  # Only show if no critical issues
            report.extend(
                [
                    "⚠️  Minor Issues (Non-blocking):",
                    "=" * 30,
                    f"Found {len(minor_warnings)} minor issues (optional columns, etc.)",
                    "These won't break your API but might be worth reviewing.",
                    "",
                ]
            )

            # Only show first few minor issues
            for issue in minor_warnings[:3]:
                report.append(f"  • {issue.table}.{issue.column}: {issue.message}")

            if len(minor_warnings) > 3:
                report.append(f"  ... and {len(minor_warnings) - 3} more minor issues")
            report.append("")

        # Overall status - make it very clear
        if critical_errors:
            status_lines = [
                "",
                "=" * 60,
                "❌ VALIDATION FAILED ❌",
                "",
                f"🚨 {len(critical_errors)} CRITICAL ISSUE(S) FOUND",
                "",
                "Your FastAPI will break at runtime!",
                "Fix the missing tables/columns above before deploying.",
                "=" * 60,
            ]
        else:
            status_lines = [
                "",
                "=" * 60,
                "✅ VALIDATION PASSED ✅",
                "",
                "All FastAPI requirements are met by DBT models.",
                "Safe to deploy!",
                "=" * 60,
            ]

        report.extend(status_lines)

        return "\n".join(report)

    def has_blocking_issues(self) -> bool:
        """Check if there are any blocking (ERROR level) issues"""
        return any(issue.severity == ValidationSeverity.ERROR for issue in self.issues)


def main():
    """Main CLI entry point"""

    if len(sys.argv) != 3:
        print("Usage: python yaml_contract_validator.py <fastapi_yaml> <dbt_yaml>")
        print("")
        print("Example:")
        print(
            "  python yaml_contract_validator.py pydantic_generated_contract.yml dbt_generated_contract.yml"
        )
        sys.exit(1)

    fastapi_yaml_path = sys.argv[1]
    dbt_yaml_path = sys.argv[2]

    print(f"🔧 Configuration:")
    print(f"   FastAPI YAML: {fastapi_yaml_path}")
    print(f"   DBT YAML: {dbt_yaml_path}")
    print("")

    validator = YAMLContractValidator(
        fastapi_yaml_path=fastapi_yaml_path, dbt_yaml_path=dbt_yaml_path
    )

    try:
        success, issues = validator.validate_contracts()

        # Generate and print report
        report = validator.generate_report()
        print(report)

        # Quick summary for debugging
        critical_issues = [i for i in issues if i.severity == ValidationSeverity.ERROR]
        if critical_issues:
            print("\n🎯 QUICK SUMMARY - CRITICAL ISSUES:")
            for issue in critical_issues:
                print(
                    f"   💥 {issue.table}: FastAPI needs this but DBT doesn't provide it"
                )

        # Exit with appropriate code
        if validator.has_blocking_issues():
            print("\n💥 Exiting with error code due to blocking issues")
            sys.exit(1)
        else:
            print("\n🚀 Validation completed successfully")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n❌ Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Validation failed with exception: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
