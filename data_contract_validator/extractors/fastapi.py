# data_contract_validator/extractors/fastapi.py
"""
FastAPI/Pydantic schema extractor - simplified version of your working code.
"""

import ast
import re
import requests
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, get_type_hints

from .base import BaseExtractor
from ..core.models import Schema


class FastAPIExtractor(BaseExtractor):
    """Extract schemas from FastAPI/Pydantic models."""
    
    def __init__(self, content: str, source: str = "unknown"):
        self.content = content
        self.source = source
    
    @classmethod
    def from_local_file(cls, file_path: str) -> "FastAPIExtractor":
        """Create extractor from local file."""
        file_path = Path(file_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return cls(content, source=f"local:{file_path}")
    
    @classmethod
    def from_github_repo(cls, repo: str, path: str, token: str = None) -> "FastAPIExtractor":
        """Create extractor from GitHub repository."""
        content = cls._fetch_github_file(repo, path, token)
        if not content:
            raise ValueError(f"Could not fetch {repo}/{path} from GitHub")
        return cls(content, source=f"github:{repo}/{path}")
    
    @staticmethod
    def _fetch_github_file(repo: str, path: str, token: str = None) -> Optional[str]:
        """Fetch file content from GitHub API."""
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {}
        
        if token:
            headers["Authorization"] = f"token {token}"
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                import base64
                content = base64.b64decode(response.json()["content"]).decode('utf-8')
                print(f"   ✅ Downloaded {path} from {repo}")
                return content
            else:
                print(f"   ❌ GitHub API error: {response.status_code}")
                return None
        except Exception as e:
            print(f"   ❌ Error fetching from GitHub: {e}")
            return None
    
    def extract_schemas(self) -> Dict[str, Schema]:
        """Extract schemas from FastAPI/Pydantic models."""
        print(f"🔍 Extracting FastAPI schemas from {self.source}")
        
        try:
            schemas = self._parse_pydantic_models(self.content)
            print(f"   ✅ Found {len(schemas)} models")
            return schemas
        except Exception as e:
            print(f"   ❌ Error parsing models: {e}")
            return {}
    
    def _parse_pydantic_models(self, content: str) -> Dict[str, Schema]:
        """Parse Pydantic models from Python code."""
        try:
            tree = ast.parse(content)
            schemas = {}
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # Check if it's a Pydantic model
                    if self._is_pydantic_model(node):
                        schema = self._analyze_pydantic_class(node)
                        if schema:
                            table_name = schema.name
                            schemas[table_name] = schema
                            print(f"   ✅ Found model: {node.name} -> {table_name}")
            
            return schemas
            
        except Exception as e:
            print(f"   ❌ Error parsing Python code: {e}")
            return {}
    
    def _is_pydantic_model(self, node: ast.ClassDef) -> bool:
        """Check if class inherits from BaseModel or SQLModel."""
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id in ["BaseModel", "SQLModel"]:
                return True
            elif isinstance(base, ast.Attribute) and base.attr in ["BaseModel", "SQLModel"]:
                return True
        return False
    
    def _analyze_pydantic_class(self, node: ast.ClassDef) -> Optional[Schema]:
        """Analyze a Pydantic class to extract schema."""
        # Convert class name to table name
        table_name = self._class_to_table_name(node.name)
        
        # Skip SQLModel tables (database models, not API models)
        if self._is_sqlmodel_table(node):
            return None
        
        columns = []
        
        # Parse type annotations
        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                field_name = item.target.id
                field_type = self._parse_type_annotation(item.annotation)
                is_required = not self._is_optional_type(item.annotation)
                
                columns.append({
                    "name": field_name,
                    "type": self._python_to_sql_type(field_type),
                    "required": is_required,
                    "nullable": not is_required
                })
        
        if not columns:
            return None
        
        return Schema(
            name=table_name,
            columns=columns,
            source=f"pydantic:{node.name}"
        )
    
    def _is_sqlmodel_table(self, node: ast.ClassDef) -> bool:
        """Check if this is a SQLModel table (database model, not API model)."""
        # Look for table=True in the class definition
        for base in node.bases:
            if isinstance(base, ast.Call):
                for keyword in base.keywords:
                    if (keyword.arg == "table" and 
                        isinstance(keyword.value, ast.Constant) and 
                        keyword.value.value is True):
                        return True
        return False
    
    def _class_to_table_name(self, class_name: str) -> str:
        """Convert CamelCase class name to snake_case table name."""
        # Insert underscore before capital letters
        table_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', class_name)
        table_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', table_name).lower()
        
        # Remove common suffixes
        for suffix in ['_model', '_schema', '_response', '_request']:
            if table_name.endswith(suffix):
                table_name = table_name[:-len(suffix)]
                break
        
        # Pluralize if it doesn't end with 's'
        # if not table_name.endswith('s') and not table_name.endswith('_data'):
        #     table_name += 's'
        
        return table_name
    
    def _parse_type_annotation(self, annotation) -> str:
        """Parse type annotation to string."""
        if isinstance(annotation, ast.Name):
            return annotation.id
        elif isinstance(annotation, ast.Subscript):
            if isinstance(annotation.value, ast.Name):
                # Handle Optional[Type], List[Type], etc.
                inner_type = self._parse_type_annotation(annotation.slice)
                return f"{annotation.value.id}[{inner_type}]"
        elif isinstance(annotation, ast.Attribute):
            # Handle datetime.datetime, etc.
            if hasattr(annotation.value, 'id'):
                return f"{annotation.value.id}.{annotation.attr}"
            return annotation.attr
        
        return "unknown"
    
    def _is_optional_type(self, annotation) -> bool:
        """Check if type annotation is Optional."""
        if isinstance(annotation, ast.Subscript):
            if isinstance(annotation.value, ast.Name):
                # Check for Optional[Type] or Union[Type, None]
                if annotation.value.id in ["Optional", "Union"]:
                    return True
        return False