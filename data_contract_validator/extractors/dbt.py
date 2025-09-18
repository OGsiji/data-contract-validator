# data_contract_validator/extractors/dbt.py
"""
DBT schema extractor
"""

import json
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Optional, Any

from .base import BaseExtractor
from ..core.models import Schema


class DBTExtractor(BaseExtractor):
    """Extract schemas from DBT projects."""

    def __init__(self, project_path: str = ".", disable_manifest: bool = False):
        self.project_path = Path(project_path)
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self.models_path = self.project_path / "models"
        self.disable_manifest = disable_manifest

    def extract_schemas(self) -> Dict[str, Schema]:
        """Extract schemas from DBT project."""
        print(f"🔍 Extracting DBT schemas from {self.project_path}")

        # Try manifest first, fallback to SQL parsing
        if self._try_compile_dbt() and self.manifest_path.exists():
            print("   📋 Using manifest.json")
            return self._extract_from_manifest()
        else:
            print("   📄 Using SQL file parsing")
            return self._extract_from_sql_files()

    def _try_compile_dbt(self) -> bool:
        """Try to compile DBT project."""
        try:
            result = subprocess.run(
                ["dbt", "parse", "--project-dir", str(self.project_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.returncode == 0
        except:
            return False

    def _extract_from_manifest(self) -> Dict[str, Schema]:
        """Extract schemas from manifest.json."""
        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)

        schemas = {}
        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                model_name = node.get("alias") or node.get("name")

                columns = []
                for col_name, col_info in node.get("columns", {}).items():
                    columns.append(
                        {
                            "name": col_name,
                            "type": col_info.get("data_type", "varchar"),
                            "required": True,
                            "nullable": False,
                        }
                    )

                schemas[model_name] = Schema(
                    name=model_name, columns=columns, source="dbt_manifest"
                )

        print(f"   ✅ Found {len(schemas)} tables in manifest")
        return schemas

    def _extract_from_sql_files(self) -> Dict[str, Schema]:
        """Extract schemas from SQL files directly."""
        schemas = {}
        sql_files = list(self.models_path.rglob("*.sql"))

        print(f"   🔍 Found {len(sql_files)} SQL files to analyze")

        for sql_file in sql_files:
            model_name = sql_file.stem

            # Skip test/analysis files
            if any(skip in str(sql_file) for skip in ["tests", "analysis", "macros"]):
                continue

            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                columns = self._extract_columns_from_sql(sql_content)
                if columns:
                    schemas[model_name] = Schema(
                        name=model_name, columns=columns, source="sql_parsing"
                    )
                    print(f"   📋 {model_name}: {len(columns)} columns")

            except Exception as e:
                print(f"   ❌ Error parsing {model_name}: {e}")

        return schemas

    def _extract_columns_from_sql(self, sql_content: str) -> List[Dict[str, Any]]:
        """Extract columns from SQL content - simplified version."""
        # Remove comments and Jinja
        cleaned = re.sub(r"--.*?\n", "\n", sql_content)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"\{\{.*?\}\}", "", cleaned)

        # Find final SELECT statement
        select_matches = list(
            re.finditer(r"select\s+(.*?)\s+from", cleaned, re.DOTALL | re.IGNORECASE)
        )

        if not select_matches:
            return []

        # Use the last SELECT (after CTEs)
        select_content = select_matches[-1].group(1).strip()

        # Split by comma and parse each column
        columns = []
        column_parts = self._split_columns(select_content)

        for col_text in column_parts:
            col_text = col_text.strip()
            if col_text and col_text != "*":
                column_name = self._extract_column_name(col_text)
                if column_name:
                    columns.append(
                        {
                            "name": column_name,
                            "type": self._infer_data_type(col_text),
                            "required": True,
                            "nullable": False,
                        }
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

        # Check for AS alias
        as_match = re.search(r"\s+as\s+(\w+)$", col_text, re.IGNORECASE)
        if as_match:
            return as_match.group(1).lower()

        # Handle table.column format
        table_match = re.search(r"(\w+)\.(\w+)$", col_text)
        if table_match:
            return table_match.group(2).lower()

        # Simple column name
        simple_match = re.search(r"^(\w+)$", col_text)
        if simple_match:
            return simple_match.group(1).lower()

        # For complex expressions, try to extract alias
        parts = col_text.split()
        if len(parts) > 1 and not "(" in parts[-1]:
            return parts[-1].lower()

        return None

    def _infer_data_type(self, expression: str) -> str:
        """Infer data type from SQL expression."""
        expr_upper = expression.upper()

        if any(func in expr_upper for func in ["COUNT", "SUM", "ROW_NUMBER"]):
            return "integer"
        elif "AVG" in expr_upper:
            return "float"
        elif any(func in expr_upper for func in ["CONCAT", "UPPER", "LOWER"]):
            return "varchar"
        elif "TIMESTAMP" in expr_upper or "CURRENT_TIMESTAMP" in expr_upper:
            return "timestamp"
        elif "DATE" in expr_upper:
            return "date"
        elif any(keyword in expr_upper for keyword in ["TRUE", "FALSE", "BOOLEAN"]):
            return "boolean"
        else:
            return "varchar"
