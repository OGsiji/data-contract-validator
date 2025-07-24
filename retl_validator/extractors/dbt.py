import json
import subprocess
import re
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional


class DBTExtractor:
    """
    Enhanced DBT Schema Extractor with compilation strategies
    Supports both fast mode (SQL parsing) and full mode (compilation)
    """

    def __init__(self, project_path: str = "."):
        self.project_path = Path(project_path)
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self.models_path = self.project_path / "models"
        
        # Cache directory for performance
        self.cache_dir = Path.home() / '.retl-validator-cache' / 'dbt'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract DBT schemas using multiple methods based on mode"""

        project_path = config.get("project_path", ".")
        fast_mode = config.get("fast_mode", False)
        
        # Update paths based on config
        self.project_path = Path(project_path)
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self.models_path = self.project_path / "models"

        if fast_mode:
            print("   ⚡ Fast mode: Using SQL parsing (no compilation)")
            return self._extract_from_sql_files()

        # Full mode: Try compilation with intelligent fallback
        print("   🔄 Full mode: Attempting schema extraction with compilation...")
        
        # Check if we can use cached results first
        cache_key = self._get_cache_key()
        cached_schema = self._get_cached_schema(cache_key)
        
        if cached_schema and self._is_cache_valid(cache_key):
            print("   💾 Using cached DBT schemas")
            return cached_schema

        # Try manifest extraction (with compilation if needed)
        try:
            schema = self._extract_from_manifest()
            # Cache successful extraction
            self._cache_schema(cache_key, schema)
            return schema
        except Exception as e:
            print(f"   ⚠️  Manifest extraction failed: {e}")

        # Fallback to SQL file parsing
        try:
            print("   📄 Falling back to SQL parsing...")
            schema = self._extract_from_sql_files()
            # Cache fallback results too
            self._cache_schema(cache_key, schema)
            return schema
        except Exception as e:
            print(f"   ❌ SQL parsing failed: {e}")
            return {}

    def _extract_from_manifest(self) -> Dict[str, Any]:
        """
        Extract from DBT manifest.json with intelligent compilation
        """

        # Check if manifest exists and is recent
        if self.manifest_path.exists():
            manifest_age = time.time() - self.manifest_path.stat().st_mtime
            if manifest_age < 3600:  # Less than 1 hour old
                print("   📋 Using existing manifest.json (recent)")
                return self._load_manifest_schemas()

        # Compile DBT project with optimizations
        print("   🔄 Running optimized dbt compile...")
        self._compile_dbt_project()

        if not self.manifest_path.exists():
            raise FileNotFoundError("DBT compilation failed to generate manifest")

        return self._load_manifest_schemas()

    def _load_manifest_schemas(self) -> Dict[str, Any]:
        """Load schemas from existing manifest.json"""
        
        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)

        extracted_schemas = {}

        # Process models from manifest
        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                model_name = node.get("alias") or node.get("name")

                columns = []
                for col_name, col_info in node.get("columns", {}).items():
                    columns.append(
                        {
                            "name": col_name,
                            "type": col_info.get("data_type", "unknown"),
                            "required": True,  # DBT doesn't specify nullable by default
                            "description": col_info.get("description", ""),
                        }
                    )

                extracted_schemas[model_name] = {
                    "columns": columns,
                    "description": node.get("description", ""),
                    "materialization": node.get("config", {}).get(
                        "materialized", "view"
                    ),
                    "source_model": node.get("name"),
                }

        return extracted_schemas

    def _extract_from_sql_files(self) -> Dict[str, Any]:
        """
        Extract from SQL files directly - optimized for fast mode
        """

        model_columns = {}
        sql_files = list(self.models_path.rglob("*.sql"))

        print(f"   📋 Found {len(sql_files)} SQL files to analyze...")

        for sql_file in sql_files:
            model_name = sql_file.stem

            if self._should_skip_file(sql_file):
                continue

            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                columns = self._extract_columns_from_sql(sql_content)
                if columns:
                    model_columns[model_name] = {
                        "columns": columns,
                        "sql_file": str(sql_file.relative_to(self.project_path)),
                        "source_model": model_name,
                    }

            except Exception as e:
                print(f"   ❌ Error parsing {model_name}: {e}")

        return model_columns

    def _compile_dbt_project(self):
        """Compile DBT project with performance optimizations"""
        
        # Check if dbt is available
        try:
            subprocess.run(["dbt", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("DBT command not found. Make sure DBT is installed and in PATH.")

        try:
            # Use optimized compilation options
            cmd = [
                "dbt", "compile",
                "--no-version-check",      # Skip version check for speed
                "--quiet",                 # Reduce output noise
                "--target", "dev"          # Use dev target (usually faster)
            ]
            
            print(f"   🔄 Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=self.project_path,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
                check=True
            )
            
            print(f"   ✅ DBT compilation completed successfully")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError("DBT compilation timed out (>2 minutes). Consider using fast mode for pre-commit hooks.")
        except subprocess.CalledProcessError as e:
            error_msg = f"DBT compilation failed: {e.stderr}"
            if "profiles.yml" in e.stderr:
                error_msg += "\n💡 Hint: Make sure your DBT profiles.yml is configured correctly"
            elif "Connection" in e.stderr:
                error_msg += "\n💡 Hint: Database connection failed. Check your credentials and network"
            raise RuntimeError(error_msg)

    # Caching methods for performance
    def _get_cache_key(self) -> str:
        """Generate cache key based on model files content"""
        
        hasher = hashlib.md5()
        sql_files = sorted(self.models_path.rglob("*.sql"))
        
        # Hash all SQL files to detect changes
        for sql_file in sql_files:
            try:
                with open(sql_file, 'rb') as f:
                    hasher.update(f.read())
            except:
                continue
        
        # Include project path in hash for multi-project support
        hasher.update(str(self.project_path).encode())
        
        return hasher.hexdigest()
    
    def _get_cached_schema(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get cached schema if it exists"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            # Invalid cache file - remove it
            cache_file.unlink(missing_ok=True)
            return None
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached schema is still valid"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        if not cache_file.exists():
            return False
        
        # Cache is valid for 1 hour
        cache_age = time.time() - cache_file.stat().st_mtime
        return cache_age < 3600
    
    def _cache_schema(self, cache_key: str, schema: Dict[str, Any]):
        """Cache schema for future use"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(schema, f, indent=2)
        except IOError:
            # Failed to cache - not critical, continue
            pass

    def _should_skip_file(self, sql_file: Path) -> bool:
        """Check if we should skip this SQL file"""
        skip_directories = {"analysis", "tests", "macros", "snapshots"}
        return any(part in skip_directories for part in sql_file.parts)

    def _extract_columns_from_sql(self, sql_content: str) -> List[Dict[str, Any]]:
        """Extract columns from SQL content - your existing logic"""

        # Find the final SELECT statement (after CTEs)
        final_select = self._find_final_select(sql_content)
        if not final_select:
            return []

        # Split by comma and parse each column
        columns = []
        column_parts = self._split_columns(final_select)

        for col_text in column_parts:
            col_text = col_text.strip()
            if col_text and col_text != "*":
                column_info = self._parse_column(col_text)
                if column_info:
                    columns.append(column_info)

        return columns

    def _find_final_select(self, sql_content: str) -> Optional[str]:
        """Find the final SELECT statement, skipping CTEs"""

        # Remove DBT Jinja syntax and comments
        cleaned = re.sub(r"\{\{[^}]+\}\}", "", sql_content)
        cleaned = re.sub(r"--.*?\n", "\n", cleaned)
        cleaned = re.sub(r"/\*[\s\S]*?\*/", "", cleaned)

        # Find all SELECT...FROM patterns
        select_patterns = list(
            re.finditer(r"select\s+(.*?)\s+from", cleaned, re.DOTALL | re.IGNORECASE)
        )

        if not select_patterns:
            return None

        # Return the last SELECT (final output, not CTE)
        return select_patterns[-1].group(1).strip()

    def _split_columns(self, select_clause: str) -> List[str]:
        """Split SELECT columns by comma, handling nested functions"""

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

        # Don't forget the last column
        if current_column.strip():
            columns.append(current_column.strip())

        return columns

    def _parse_column(self, col_text: str) -> Optional[Dict[str, Any]]:
        """Parse a single column definition"""

        col_text = col_text.strip()

        # Extract column name
        column_name = self._extract_column_name(col_text)
        if not column_name:
            return None

        # Infer data type
        data_type = self._infer_data_type(col_text)

        return {
            "name": column_name,
            "type": data_type,
            "required": True,  # Default assumption for DBT
            "description": f'Generated from: {col_text[:50]}{"..." if len(col_text) > 50 else ""}',
        }

    def _extract_column_name(self, col_text: str) -> Optional[str]:
        """Extract clean column name from column definition"""

        col_text = col_text.strip()

        # Check for AS alias
        as_match = re.search(r"\s+as\s+(\w+)$", col_text, re.IGNORECASE)
        if as_match:
            return as_match.group(1).lower()

        # Handle table.column format (d.username -> username)
        table_prefix_match = re.search(r"(\w+)\.(\w+)$", col_text)
        if table_prefix_match:
            return table_prefix_match.group(2).lower()

        # Handle simple column name
        simple_match = re.search(r"^(\w+)$", col_text)
        if simple_match:
            return simple_match.group(1).lower()

        # For complex expressions, check for space-separated alias
        parts = col_text.split()
        if (
            len(parts) > 1
            and not any(
                keyword in col_text.upper()
                for keyword in ["CASE", "WHEN", "THEN", "ELSE", "END"]
            )
            and not "(" in parts[-1]
        ):
            return parts[-1].lower()

        # Generate generic name for complex expressions
        return "computed_column"

    def _infer_data_type(self, expression: str) -> str:
        """Infer data type from SQL expression"""

        expression_upper = expression.upper()

        # Function-based inference
        if any(func in expression_upper for func in ["COUNT", "SUM", "ROW_NUMBER"]):
            return "integer"
        elif "AVG" in expression_upper:
            return "float"
        elif any(
            func in expression_upper for func in ["CONCAT", "UPPER", "LOWER", "TRIM"]
        ):
            return "varchar"
        elif "TIMESTAMP" in expression_upper or "CURRENT_TIMESTAMP" in expression_upper:
            return "timestamp"
        elif "DATE" in expression_upper:
            return "date"
        elif any(
            keyword in expression_upper for keyword in ["TRUE", "FALSE", "BOOLEAN"]
        ):
            return "boolean"

        # Pattern-based inference
        if re.search(r"_id$|^id$", expression, re.IGNORECASE):
            return "varchar"
        elif re.search(r"_at$|_time$|timestamp", expression, re.IGNORECASE):
            return "timestamp"
        elif re.search(r"count|total|sum", expression, re.IGNORECASE):
            return "integer"
        elif re.search(r"rate|ratio|percentage|avg", expression, re.IGNORECASE):
            return "float"

        # Default
        return "varchar"
    
    # Utility methods for debugging and monitoring
    def get_compilation_status(self) -> Dict[str, Any]:
        """Get current compilation status for debugging"""
        
        manifest_exists = self.manifest_path.exists()
        manifest_age = None
        
        if manifest_exists:
            manifest_age = time.time() - self.manifest_path.stat().st_mtime
        
        return {
            "project_path": str(self.project_path),
            "manifest_exists": manifest_exists,
            "manifest_age_seconds": manifest_age,
            "manifest_recent": manifest_age < 3600 if manifest_age else False,
            "models_directory_exists": self.models_path.exists(),
            "sql_files_count": len(list(self.models_path.rglob("*.sql"))) if self.models_path.exists() else 0
        }