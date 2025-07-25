#!/usr/bin/env python3
"""
Enhanced DBT Extractor with Auto-Schema Update
1. Extracts columns from SQL files
2. Updates schema.yml files
3. Runs dbt compile
4. Returns complete schemas for validation
"""

import re
import os
import yaml
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Any


class EnhancedDBTExtractor:
    def __init__(self, project_path: str = "."):
        self.project_path = Path(project_path)
        self.models_path = self.project_path / "models"
        self.target_dir = self.project_path / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        
    def extract_schemas_with_auto_update(self) -> Dict[str, Any]:
        """
        Complete workflow: Extract → Update Schema → Compile → Return schemas
        """
        print("🔍 Enhanced DBT Schema Extraction with Auto-Update")
        print("=" * 55)
        
        # Step 1: Extract columns from SQL files
        print("\n📋 Step 1: Extracting columns from SQL files...")
        model_columns = self._extract_columns_from_all_models()
        
        if not model_columns:
            print("❌ No columns extracted from SQL files")
            return {}
        
        print(f"   ✅ Extracted columns from {len(model_columns)} models")
        
        # Step 2: Update schema.yml files
        print("\n📝 Step 2: Updating schema.yml files...")
        self._update_schema_files(model_columns)
        
        # Step 3: Run dbt compile to generate complete manifest
        print("\n🔄 Step 3: Running dbt compile with updated schemas...")
        if self._compile_dbt_project():
            print("   ✅ DBT compilation successful")
        else:
            print("   ⚠️  DBT compilation failed, falling back to SQL extraction")
            return self._format_sql_extracted_schemas(model_columns)
        
        # Step 4: Load complete manifest
        print("\n📊 Step 4: Loading complete manifest...")
        if self.manifest_path.exists():
            manifest_schemas = self._load_manifest_schemas()
            print(f"   ✅ Loaded {len(manifest_schemas)} models from manifest")
            return manifest_schemas
        else:
            print("   ⚠️  No manifest found, using SQL extraction")
            return self._format_sql_extracted_schemas(model_columns)
    
    def _extract_columns_from_all_models(self) -> Dict[str, Dict[str, Any]]:
        """Extract columns from all DBT model SQL files"""
        
        model_columns = {}
        sql_files = list(self.models_path.rglob("*.sql"))
        
        print(f"   🔍 Found {len(sql_files)} SQL files to analyze")
        
        for sql_file in sql_files:
            model_name = sql_file.stem
            
            # Skip analysis, tests, etc.
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
                        'sql_path': sql_file
                    }
                    print(f"   📋 {model_name}: {len(columns)} columns")
                    
            except Exception as e:
                print(f"   ❌ Error parsing {model_name}: {e}")
                
        return model_columns
    
    def _update_schema_files(self, model_columns: Dict[str, Dict[str, Any]]):
        """Update schema.yml files with discovered columns"""
        
        # Group models by directory to update appropriate schema files
        models_by_dir = {}
        for model_name, model_info in model_columns.items():
            sql_path = model_info['sql_path']
            model_dir = sql_path.parent
            
            if model_dir not in models_by_dir:
                models_by_dir[model_dir] = {}
            models_by_dir[model_dir][model_name] = model_info
        
        # Update schema files for each directory
        total_updated = 0
        for model_dir, dir_models in models_by_dir.items():
            schema_file = model_dir / "_schema.yml"
            
            # Try different schema file names
            if not schema_file.exists():
                for schema_name in ["schema.yml", "models.yml", "_models.yml"]:
                    potential_schema = model_dir / schema_name
                    if potential_schema.exists():
                        schema_file = potential_schema
                        break
            
            updated = self._update_single_schema_file(schema_file, dir_models)
            total_updated += updated
            
        print(f"   ✅ Updated {total_updated} columns across schema files")
    
    def _update_single_schema_file(self, schema_file: Path, dir_models: Dict[str, Dict[str, Any]]) -> int:
        """Update a single schema.yml file"""
        
        # Load existing schema or create new one
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                schema_content = yaml.safe_load(f) or {}
        else:
            schema_content = {'version': 2, 'models': []}
            print(f"   📝 Creating new schema file: {schema_file}")
        
        if 'models' not in schema_content:
            schema_content['models'] = []
        
        # Build lookup of existing models
        existing_models = {model['name']: model for model in schema_content['models']}
        
        updated_count = 0
        for model_name, model_info in dir_models.items():
            if model_name in existing_models:
                # Update existing model
                existing_model = existing_models[model_name]
                if 'columns' not in existing_model:
                    existing_model['columns'] = []
                
                existing_columns = {col['name']: col for col in existing_model['columns']}
                
                # Add missing columns
                for column in model_info['columns']:
                    if column['name'] not in existing_columns:
                        existing_model['columns'].append({
                            'name': column['name'],
                            'data_type': column['data_type'],
                            'description': column['description']
                        })
                        updated_count += 1
            else:
                # Add new model
                new_model = {
                    'name': model_name,
                    'description': f'Auto-generated schema for {model_name}',
                    'columns': [
                        {
                            'name': col['name'],
                            'data_type': col['data_type'],
                            'description': col['description']
                        }
                        for col in model_info['columns']
                    ]
                }
                schema_content['models'].append(new_model)
                updated_count += len(model_info['columns'])
        
        # Write updated schema
        if updated_count > 0:
            with open(schema_file, 'w') as f:
                yaml.dump(schema_content, f, default_flow_style=False, sort_keys=False, indent=2)
            
            print(f"   📝 Updated {schema_file.name}: +{updated_count} columns")
        
        return updated_count
    
    def _compile_dbt_project(self) -> bool:
        """Compile DBT project to generate complete manifest"""
        try:
            result = subprocess.run(
                ["dbt", "compile", "--quiet"],
                cwd=self.project_path,
                capture_output=True,
                text=True,
                timeout=120
            )
            return result.returncode == 0
        except Exception as e:
            print(f"   ❌ DBT compilation error: {e}")
            return False
    
    def _load_manifest_schemas(self) -> Dict[str, Any]:
        """Load schemas from manifest.json"""
        with open(self.manifest_path, "r") as f:
            manifest = json.load(f)

        extracted_schemas = {}

        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                model_name = node.get("alias") or node.get("name")

                columns = []
                for col_name, col_info in node.get("columns", {}).items():
                    columns.append({
                        "name": col_name,
                        "type": col_info.get("data_type", "unknown"),
                        "required": True,
                        "description": col_info.get("description", ""),
                    })

                extracted_schemas[model_name] = {
                    "table_name": model_name,
                    "columns": columns,
                    "description": node.get("description", ""),
                    "materialization": node.get("config", {}).get("materialized", "view"),
                    "source_model": node.get("name"),
                    "source": "manifest"
                }

        return extracted_schemas
    
    def _format_sql_extracted_schemas(self, model_columns: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Format SQL extracted columns as schemas"""
        formatted_schemas = {}
        
        for model_name, model_info in model_columns.items():
            columns = []
            for col in model_info['columns']:
                columns.append({
                    "name": col['name'],
                    "type": col['data_type'],
                    "required": True,
                    "description": col['description']
                })
            
            formatted_schemas[model_name] = {
                "table_name": model_name,
                "columns": columns,
                "source_model": model_name,
                "source": "sql_extraction"
            }
        
        return formatted_schemas
    
    # Include all the existing helper methods from your script
    def _should_skip_file(self, sql_file: Path) -> bool:
        """Check if we should skip this SQL file"""
        skip_directories = {'analysis', 'tests', 'macros', 'snapshots'}
        return any(part in skip_directories for part in sql_file.parts)
    
    def _extract_columns_from_sql(self, sql_content: str) -> List[Dict[str, Any]]:
        """Extract columns from SQL content"""
        final_select = self._find_final_select(sql_content)
        if not final_select:
            return []

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
        cleaned = re.sub(r'\{\{[^}]+\}\}', '', sql_content)
        cleaned = re.sub(r'--.*?\n', '\n', cleaned)
        cleaned = re.sub(r'/\*[\s\S]*?\*/', '', cleaned)

        select_patterns = list(re.finditer(
            r'select\s+(.*?)\s+from', 
            cleaned, 
            re.DOTALL | re.IGNORECASE
        ))

        if not select_patterns:
            return None

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

        if current_column.strip():
            columns.append(current_column.strip())

        return columns
    
    def _parse_column(self, col_text: str) -> Optional[Dict[str, Any]]:
        """Parse a single column definition"""
        col_text = col_text.strip()

        column_name = self._extract_column_name(col_text)
        if not column_name:
            return None

        data_type = self._infer_data_type(col_text)
        description = f'Generated from: {col_text[:50]}{"..." if len(col_text) > 50 else ""}'

        return {
            'name': column_name,
            'data_type': data_type,
            'description': description
        }
    
    def _extract_column_name(self, col_text: str) -> Optional[str]:
        """Extract clean column name from column definition"""
        col_text = col_text.strip()

        # Check for AS alias
        as_match = re.search(r'\s+as\s+(\w+)$', col_text, re.IGNORECASE)
        if as_match:
            return as_match.group(1).lower()

        # Handle table.column format
        table_prefix_match = re.search(r'(\w+)\.(\w+)$', col_text)
        if table_prefix_match:
            return table_prefix_match.group(2).lower()

        # Handle simple column name
        simple_match = re.search(r'^(\w+)$', col_text)
        if simple_match:
            return simple_match.group(1).lower()

        # For complex expressions
        parts = col_text.split()
        if (len(parts) > 1 and 
            not any(keyword in col_text.upper() for keyword in ['CASE', 'WHEN', 'THEN', 'ELSE', 'END']) and
            not '(' in parts[-1]):
            return parts[-1].lower()

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

        return 'varchar'


# Update the original SimpleDbtExtractor to use this enhanced version
class SimpleDbtExtractor:
    def __init__(self, project_path: str = "."):
        self.enhanced_extractor = EnhancedDBTExtractor(project_path)
        
    def extract_schemas(self) -> Dict[str, Any]:
        """
        Extract schemas using enhanced approach:
        1. Extract from SQL
        2. Update schema.yml
        3. Compile DBT
        4. Return complete schemas
        """
        return self.enhanced_extractor.extract_schemas_with_auto_update()


if __name__ == "__main__":
    # Test the enhanced extractor
    extractor = EnhancedDBTExtractor()
    schemas = extractor.extract_schemas_with_auto_update()
    
    print(f"\n📊 Final Results:")
    print(f"   Total schemas: {len(schemas)}")
    for name, schema in schemas.items():
        print(f"   • {name}: {len(schema['columns'])} columns")