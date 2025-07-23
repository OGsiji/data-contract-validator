import json
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Any, Optional

class DBTExtractor:
    """
    DBT Schema Extractor - Combines your existing DBT extraction logic
    Based on your generate_column_schema.py and dbt_generate_manifest_change_schema.py
    """
    
    def __init__(self, project_path: str = "."):
        self.project_path = Path(project_path)
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self.models_path = self.project_path / "models"
    
    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract DBT schemas using multiple methods"""
        
        project_path = config.get('project_path', '.')
        self.project_path = Path(project_path)
        
        # Method 1: Try manifest.json (most accurate)
        try:
            return self._extract_from_manifest()
        except Exception as e:
            print(f"   ⚠️  Manifest extraction failed: {e}")
            
        # Method 2: Fallback to SQL file parsing (faster for CI)
        try:
            return self._extract_from_sql_files()
        except Exception as e:
            print(f"   ❌ SQL parsing failed: {e}")
            return {}
    
    def _extract_from_manifest(self) -> Dict[str, Any]:
        """
        Extract from DBT manifest.json - Your generate_column_schema.py logic
        """
        
        # Try to compile first if manifest doesn't exist
        if not self.manifest_path.exists():
            self._compile_dbt_project()
        
        if not self.manifest_path.exists():
            raise FileNotFoundError("No DBT manifest found and compilation failed")
        
        with open(self.manifest_path, 'r') as f:
            manifest = json.load(f)
        
        extracted_schemas = {}
        
        # Process models from manifest
        for node_id, node in manifest.get('nodes', {}).items():
            if node.get('resource_type') == 'model':
                model_name = node.get('alias') or node.get('name')
                
                columns = []
                for col_name, col_info in node.get('columns', {}).items():
                    columns.append({
                        'name': col_name,
                        'type': col_info.get('data_type', 'unknown'),
                        'required': True,  # DBT doesn't specify nullable by default
                        'description': col_info.get('description', '')
                    })
                
                extracted_schemas[model_name] = {
                    'columns': columns,
                    'description': node.get('description', ''),
                    'materialization': node.get('config', {}).get('materialized', 'view'),
                    'source_model': node.get('name')
                }
        
        return extracted_schemas
    
    def _extract_from_sql_files(self) -> Dict[str, Any]:
        """
        Extract from SQL files directly - Your dbt_generate_manifest_change_schema.py logic
        """
        
        model_columns = {}
        sql_files = list(self.models_path.rglob("*.sql"))
        
        print(f"   📋 Found {len(sql_files)} SQL files to analyze...")
        
        for sql_file in sql_files:
            model_name = sql_file.stem
            
            if self._should_skip_file(sql_file):
                continue
            
            try:
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                columns = self._extract_columns_from_sql(sql_content)
                if columns:
                    model_columns[model_name] = {
                        'columns': columns,
                        'sql_file': str(sql_file.relative_to(self.project_path)),
                        'source_model': model_name
                    }
                    
            except Exception as e:
                print(f"   ❌ Error parsing {model_name}: {e}")
        
        return model_columns
    
    def _compile_dbt_project(self):
        """Compile DBT project to generate manifest"""
        try:
            result = subprocess.run(
                ["dbt", "compile"],
                cwd=self.project_path,
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"DBT compilation failed: {e.stderr}")
    
    def _should_skip_file(self, sql_file: Path) -> bool:
        """Check if we should skip this SQL file"""
        skip_directories = {'analysis', 'tests', 'macros', 'snapshots'}
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
            if col_text and col_text != '*':
                column_info = self._parse_column(col_text)
                if column_info:
                    columns.append(column_info)
        
        return columns
    
    def _find_final_select(self, sql_content: str) -> Optional[str]:
        """Find the final SELECT statement, skipping CTEs"""
        
        # Remove DBT Jinja syntax and comments
        cleaned = re.sub(r'\{\{[^}]+\}\}', '', sql_content)
        cleaned = re.sub(r'--.*?\n', '\n', cleaned)
        cleaned = re.sub(r'/\*[\s\S]*?\*/', '', cleaned)
        
        # Find all SELECT...FROM patterns
        select_patterns = list(re.finditer(
            r'select\s+(.*?)\s+from', 
            cleaned, 
            re.DOTALL | re.IGNORECASE
        ))
        
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
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
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
            'name': column_name,
            'type': data_type,
            'required': True,  # Default assumption for DBT
            'description': f'Generated from: {col_text[:50]}{"..." if len(col_text) > 50 else ""}'
        }
    
    def _extract_column_name(self, col_text: str) -> Optional[str]:
        """Extract clean column name from column definition"""
        
        col_text = col_text.strip()
        
        # Check for AS alias
        as_match = re.search(r'\s+as\s+(\w+)$', col_text, re.IGNORECASE)
        if as_match:
            return as_match.group(1).lower()
        
        # Handle table.column format (d.username -> username)
        table_prefix_match = re.search(r'(\w+)\.(\w+)$', col_text)
        if table_prefix_match:
            return table_prefix_match.group(2).lower()
        
        # Handle simple column name
        simple_match = re.search(r'^(\w+)$', col_text)
        if simple_match:
            return simple_match.group(1).lower()
        
        # For complex expressions, check for space-separated alias
        parts = col_text.split()
        if (len(parts) > 1 and 
            not any(keyword in col_text.upper() for keyword in ['CASE', 'WHEN', 'THEN', 'ELSE', 'END']) and
            not '(' in parts[-1]):
            return parts[-1].lower()
        
        # Generate generic name for complex expressions
        return 'computed_column'
    
    def _infer_data_type(self, expression: str) -> str:
        """Infer data type from SQL expression"""
        
        expression_upper = expression.upper()
        
        # Function-based inference
        if any(func in expression_upper for func in ['COUNT', 'SUM', 'ROW_NUMBER']):
            return 'integer'
        elif 'AVG' in expression_upper:
            return 'float'
        elif any(func in expression_upper for func in ['CONCAT', 'UPPER', 'LOWER', 'TRIM']):
            return 'varchar'
        elif 'TIMESTAMP' in expression_upper or 'CURRENT_TIMESTAMP' in expression_upper:
            return 'timestamp'
        elif 'DATE' in expression_upper:
            return 'date'
        elif any(keyword in expression_upper for keyword in ['TRUE', 'FALSE', 'BOOLEAN']):
            return 'boolean'
        
        # Pattern-based inference
        if re.search(r'_id$|^id$', expression, re.IGNORECASE):
            return 'varchar'
        elif re.search(r'_at$|_time$|timestamp', expression, re.IGNORECASE):
            return 'timestamp'
        elif re.search(r'count|total|sum', expression, re.IGNORECASE):
            return 'integer'
        elif re.search(r'rate|ratio|percentage|avg', expression, re.IGNORECASE):
            return 'float'
        
        # Default
        return 'varchar'