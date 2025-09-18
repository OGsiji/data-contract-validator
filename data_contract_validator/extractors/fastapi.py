# data_contract_validator/extractors/fastapi.py
"""
Enhanced FastAPI/Pydantic schema extractor with directory support
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
    """Extract schemas from FastAPI/Pydantic models - supports files and directories."""

    def __init__(
        self, content: str = None, source: str = "unknown", file_path: str = None
    ):
        self.content = content
        self.source = source
        self.file_path = file_path
        self.all_files_content = {}  # For directory mode

    @classmethod
    def from_local_file(cls, file_path: str) -> "FastAPIExtractor":
        """Create extractor from local file."""
        file_path = Path(file_path)

        if not file_path.exists():
            raise ValueError(f"Path does not exist: {file_path}")

        if file_path.is_file():
            # Single file mode (existing behavior)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            return cls(
                content=content, source=f"local:{file_path}", file_path=str(file_path)
            )

        elif file_path.is_dir():
            # Directory mode (new functionality)
            return cls._from_local_directory(file_path)

        else:
            raise ValueError(f"Path is neither file nor directory: {file_path}")

    @classmethod
    def from_local_directory(cls, directory_path: str) -> "FastAPIExtractor":
        """Create extractor from local directory containing model files."""
        return cls._from_local_directory(Path(directory_path))

    @classmethod
    def _from_local_directory(cls, dir_path: Path) -> "FastAPIExtractor":
        """Internal method to handle directory extraction."""
        if not dir_path.is_dir():
            raise ValueError(f"Not a directory: {dir_path}")

        # Find all Python files in the directory and subdirectories
        python_files = list(dir_path.rglob("*.py"))

        if not python_files:
            raise ValueError(f"No Python files found in directory: {dir_path}")

        print(f"🔍 Found {len(python_files)} Python files in {dir_path}")

        # Read all files
        all_files_content = {}
        for py_file in python_files:
            # Skip common non-model files
            if py_file.name in [
                "__init__.py",
                "test_",
                "tests.py",
            ] or py_file.name.startswith("test_"):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    relative_path = py_file.relative_to(dir_path)
                    all_files_content[str(relative_path)] = content
                    print(f"   📄 Loaded: {relative_path}")
            except Exception as e:
                print(f"   ⚠️  Could not read {py_file}: {e}")

        if not all_files_content:
            raise ValueError(f"Could not read any Python files from: {dir_path}")

        # Create extractor instance for directory mode
        extractor = cls(source=f"local_directory:{dir_path}")
        extractor.all_files_content = all_files_content
        return extractor

    @classmethod
    def from_github_repo(
        cls, repo: str, path: str, token: str = None
    ) -> "FastAPIExtractor":
        """Create extractor from GitHub repository - supports files and directories."""

        # First, check if it's a file or directory
        if path.endswith(".py"):
            # Single file
            content = cls._fetch_github_file(repo, path, token)
            if not content:
                raise ValueError(f"Could not fetch {repo}/{path} from GitHub")
            return cls(content, source=f"github:{repo}/{path}")
        else:
            # Assume it's a directory
            return cls._from_github_directory(repo, path, token)

    @classmethod
    def _from_github_directory(
        cls, repo: str, dir_path: str, token: str = None
    ) -> "FastAPIExtractor":
        """Fetch all Python files from a GitHub directory."""

        # Get directory contents from GitHub API
        url = f"https://api.github.com/repos/{repo}/contents/{dir_path}"
        headers = {}

        if token:
            headers["Authorization"] = f"token {token}"

        try:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise ValueError(
                    f"Could not fetch directory {repo}/{dir_path}: {response.status_code}"
                )

            contents = response.json()
            if not isinstance(contents, list):
                raise ValueError(f"Path {dir_path} is not a directory")

            all_files_content = {}

            for item in contents:
                if item["type"] == "file" and item["name"].endswith(".py"):
                    # Skip common non-model files
                    if item["name"] in ["__init__.py"] or item["name"].startswith(
                        "test_"
                    ):
                        continue

                    file_content = cls._fetch_github_file(repo, item["path"], token)
                    if file_content:
                        all_files_content[item["name"]] = file_content
                        print(f"   📄 Downloaded: {item['name']}")

                elif item["type"] == "dir":
                    # Recursively fetch subdirectories
                    try:
                        subdir_files = cls._fetch_github_directory_recursive(
                            repo, item["path"], token
                        )
                        for sub_path, sub_content in subdir_files.items():
                            all_files_content[f"{item['name']}/{sub_path}"] = (
                                sub_content
                            )
                    except Exception as e:
                        print(f"   ⚠️  Could not fetch subdirectory {item['name']}: {e}")

            if not all_files_content:
                raise ValueError(f"No Python model files found in {repo}/{dir_path}")

            print(
                f"   ✅ Downloaded {len(all_files_content)} files from {repo}/{dir_path}"
            )

            extractor = cls(source=f"github_directory:{repo}/{dir_path}")
            extractor.all_files_content = all_files_content
            return extractor

        except Exception as e:
            raise ValueError(f"Error fetching GitHub directory {repo}/{dir_path}: {e}")

    @classmethod
    def _fetch_github_directory_recursive(
        cls, repo: str, dir_path: str, token: str = None
    ) -> Dict[str, str]:
        """Recursively fetch Python files from GitHub directory."""
        url = f"https://api.github.com/repos/{repo}/contents/{dir_path}"
        headers = {}

        if token:
            headers["Authorization"] = f"token {token}"

        files_content = {}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                contents = response.json()

                for item in contents:
                    if item["type"] == "file" and item["name"].endswith(".py"):
                        if (
                            not item["name"].startswith("test_")
                            and item["name"] != "__init__.py"
                        ):
                            file_content = cls._fetch_github_file(
                                repo, item["path"], token
                            )
                            if file_content:
                                files_content[item["name"]] = file_content

                    elif item["type"] == "dir":
                        # Recursive call for subdirectories
                        subdir_files = cls._fetch_github_directory_recursive(
                            repo, item["path"], token
                        )
                        for sub_path, sub_content in subdir_files.items():
                            files_content[f"{item['name']}/{sub_path}"] = sub_content

        except Exception as e:
            print(f"   ⚠️  Error fetching subdirectory {dir_path}: {e}")

        return files_content

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

                content = base64.b64decode(response.json()["content"]).decode("utf-8")
                return content
            else:
                print(f"   ❌ GitHub API error for {path}: {response.status_code}")
                return None
        except Exception as e:
            print(f"   ❌ Error fetching {path} from GitHub: {e}")
            return None

    def extract_schemas(self) -> Dict[str, Schema]:
        """Extract schemas from FastAPI/Pydantic models."""

        if self.all_files_content:
            # Directory mode - extract from multiple files
            return self._extract_schemas_from_directory()
        else:
            # Single file mode - existing behavior
            return self._extract_schemas_from_single_file()

    def _extract_schemas_from_single_file(self) -> Dict[str, Schema]:
        """Extract schemas from a single file (existing behavior)."""
        print(f"🔍 Extracting FastAPI schemas from {self.source}")

        try:
            schemas = self._parse_pydantic_models(self.content)
            print(f"   ✅ Found {len(schemas)} models")
            return schemas
        except Exception as e:
            print(f"   ❌ Error parsing models: {e}")
            return {}

    def _extract_schemas_from_directory(self) -> Dict[str, Schema]:
        """Extract schemas from multiple files in a directory."""
        print(f"🔍 Extracting FastAPI schemas from directory {self.source}")

        all_schemas = {}
        total_models = 0

        for file_path, file_content in self.all_files_content.items():
            try:
                print(f"   📄 Processing: {file_path}")
                file_schemas = self._parse_pydantic_models(
                    file_content, file_source=file_path
                )

                # Check for duplicate model names across files
                for schema_name, schema in file_schemas.items():
                    if schema_name in all_schemas:
                        print(
                            f"   ⚠️  Duplicate model name '{schema_name}' found in {file_path}"
                        )
                        print(f"       Previous: {all_schemas[schema_name].source}")
                        print(f"       Current:  {schema.source}")
                        # Use a unique name by including file path
                        unique_name = f"{schema_name}_{file_path.replace('/', '_').replace('.py', '')}"
                        all_schemas[unique_name] = schema
                        print(f"       Renamed to: {unique_name}")
                    else:
                        all_schemas[schema_name] = schema

                if file_schemas:
                    print(f"       ✅ Found {len(file_schemas)} models")
                    total_models += len(file_schemas)
                else:
                    print(f"       ⚪ No Pydantic models found")

            except Exception as e:
                print(f"   ❌ Error parsing {file_path}: {e}")

        print(
            f"   ✅ Total: {total_models} models from {len(self.all_files_content)} files"
        )
        return all_schemas

    def _parse_pydantic_models(
        self, content: str, file_source: str = None
    ) -> Dict[str, Schema]:
        """Parse Pydantic models from Python code."""
        try:
            tree = ast.parse(content)
            schemas = {}

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # Check if it's a Pydantic model
                    if self._is_pydantic_model(node):
                        schema = self._analyze_pydantic_class(node, file_source)
                        if schema:
                            table_name = schema.name
                            schemas[table_name] = schema

            return schemas

        except Exception as e:
            print(f"   ❌ Error parsing Python code: {e}")
            return {}

    def _is_pydantic_model(self, node: ast.ClassDef) -> bool:
        """Check if class inherits from BaseModel or SQLModel."""
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id in ["BaseModel", "SQLModel"]:
                return True
            elif isinstance(base, ast.Attribute) and base.attr in [
                "BaseModel",
                "SQLModel",
            ]:
                return True
        return False

    def _analyze_pydantic_class(
        self, node: ast.ClassDef, file_source: str = None
    ) -> Optional[Schema]:
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

                columns.append(
                    {
                        "name": field_name,
                        "type": self._python_to_sql_type(field_type),
                        "required": is_required,
                        "nullable": not is_required,
                    }
                )

        if not columns:
            return None

        # Create source identifier
        if file_source:
            source = f"pydantic:{node.name}@{file_source}"
        else:
            source = f"pydantic:{node.name}"

        return Schema(name=table_name, columns=columns, source=source)

    def _is_sqlmodel_table(self, node: ast.ClassDef) -> bool:
        """Check if this is a SQLModel table (database model, not API model)."""
        # Look for table=True in the class definition
        for base in node.bases:
            if isinstance(base, ast.Call):
                for keyword in base.keywords:
                    if (
                        keyword.arg == "table"
                        and isinstance(keyword.value, ast.Constant)
                        and keyword.value.value is True
                    ):
                        return True
        return False

    def _class_to_table_name(self, class_name: str) -> str:
        """Convert CamelCase class name to snake_case table name."""
        # Insert underscore before capital letters
        table_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", class_name)
        table_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", table_name).lower()

        # Remove common suffixes
        for suffix in ["_model", "_schema", "_response", "_request"]:
            if table_name.endswith(suffix):
                table_name = table_name[: -len(suffix)]
                break

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
            if hasattr(annotation.value, "id"):
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
