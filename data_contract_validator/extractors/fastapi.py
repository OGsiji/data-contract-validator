# data_contract_validator/extractors/fastapi.py
"""
Enhanced FastAPI/Pydantic schema extractor with directory support
"""

import ast
import re
import requests
import os
from collections.abc import Mapping
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
        cls, repo: str, path: str, token: str = None, ref: str = None
    ) -> "FastAPIExtractor":
        """Create extractor from GitHub repository - supports files and directories.

        Args:
            ref: Branch, tag, or commit SHA to read from. Defaults to the
                repo's default branch when omitted (GitHub API behavior).
                Useful for validating a dev/staging branch's API models
                against dbt instead of main.
        """

        # First, check if it's a file or directory
        if path.endswith(".py"):
            # Single file
            content = cls._fetch_github_file(repo, path, token, ref)
            if not content:
                raise ValueError(f"Could not fetch {repo}/{path} from GitHub")
            source = f"github:{repo}/{path}" + (f"@{ref}" if ref else "")
            return cls(content, source=source)
        else:
            # Assume it's a directory
            return cls._from_github_directory(repo, path, token, ref)

    @classmethod
    def _from_github_directory(
        cls, repo: str, dir_path: str, token: str = None, ref: str = None
    ) -> "FastAPIExtractor":
        """Fetch all Python files from a GitHub directory."""

        # Get directory contents from GitHub API
        url = f"https://api.github.com/repos/{repo}/contents/{dir_path}"
        headers = {}
        params = {"ref": ref} if ref else {}

        if token:
            headers["Authorization"] = f"token {token}"

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                raise ValueError(
                    f"Could not fetch directory {repo}/{dir_path}: "
                    f"{response.status_code}{cls._github_auth_hint(response.status_code, token)}"
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

                    file_content = cls._fetch_github_file(
                        repo, item["path"], token, ref
                    )
                    if file_content:
                        all_files_content[item["name"]] = file_content
                        print(f"   📄 Downloaded: {item['name']}")

                elif item["type"] == "dir":
                    # Recursively fetch subdirectories
                    try:
                        subdir_files = cls._fetch_github_directory_recursive(
                            repo, item["path"], token, ref
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

            source = f"github_directory:{repo}/{dir_path}" + (f"@{ref}" if ref else "")
            extractor = cls(source=source)
            extractor.all_files_content = all_files_content
            return extractor

        except Exception as e:
            raise ValueError(f"Error fetching GitHub directory {repo}/{dir_path}: {e}")

    @classmethod
    def _fetch_github_directory_recursive(
        cls, repo: str, dir_path: str, token: str = None, ref: str = None
    ) -> Dict[str, str]:
        """Recursively fetch Python files from GitHub directory."""
        url = f"https://api.github.com/repos/{repo}/contents/{dir_path}"
        headers = {}
        params = {"ref": ref} if ref else {}

        if token:
            headers["Authorization"] = f"token {token}"

        files_content = {}

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                contents = response.json()

                for item in contents:
                    if item["type"] == "file" and item["name"].endswith(".py"):
                        if (
                            not item["name"].startswith("test_")
                            and item["name"] != "__init__.py"
                        ):
                            file_content = cls._fetch_github_file(
                                repo, item["path"], token, ref
                            )
                            if file_content:
                                files_content[item["name"]] = file_content

                    elif item["type"] == "dir":
                        # Recursive call for subdirectories
                        subdir_files = cls._fetch_github_directory_recursive(
                            repo, item["path"], token, ref
                        )
                        for sub_path, sub_content in subdir_files.items():
                            files_content[f"{item['name']}/{sub_path}"] = sub_content

        except Exception as e:
            print(f"   ⚠️  Error fetching subdirectory {dir_path}: {e}")

        return files_content

    @staticmethod
    def _fetch_github_file(
        repo: str, path: str, token: str = None, ref: str = None
    ) -> Optional[str]:
        """Fetch file content from GitHub API with rate limit handling."""
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {}
        params = {"ref": ref} if ref else {}

        if token:
            headers["Authorization"] = f"token {token}"

        try:
            response = requests.get(url, headers=headers, params=params)

            # Check rate limit headers (defensively -- headers may not be a dict).
            response_headers = getattr(response, "headers", None)
            remaining_raw = (
                response_headers.get("X-RateLimit-Remaining")
                if isinstance(response_headers, Mapping)
                else None
            )
            if remaining_raw is not None:
                remaining = int(remaining_raw)
                if remaining < 10:
                    print(
                        f"   ⚠️  GitHub API rate limit low: {remaining} requests remaining"
                    )
                    if remaining == 0:
                        reset_time = int(response_headers.get("X-RateLimit-Reset", 0))
                        import time

                        wait_time = max(0, reset_time - int(time.time()))
                        print(
                            f"   ⏳ Rate limit exceeded. Resets in {wait_time // 60} minutes"
                        )

            if response.status_code == 200:
                import base64

                content = base64.b64decode(response.json()["content"]).decode("utf-8")
                return content
            elif response.status_code == 403:
                # Check if it's a rate limit error
                error_message = response.json().get("message", "")
                if "rate limit" in error_message.lower():
                    print(f"   ❌ GitHub API rate limit exceeded")
                    print(
                        f"   💡 Try setting GITHUB_TOKEN environment variable for higher limits"
                    )
                else:
                    print(f"   ❌ GitHub API access forbidden: {error_message}")
                return None
            elif response.status_code == 404:
                print(
                    f"   ❌ File not found: {path}"
                    f"{FastAPIExtractor._github_auth_hint(404, token)}"
                )
                return None
            else:
                print(f"   ❌ GitHub API error for {path}: {response.status_code}")
                return None
        except Exception as e:
            print(f"   ❌ Error fetching {path} from GitHub: {e}")
            return None

    @staticmethod
    def _github_auth_hint(status_code: int, token: Optional[str]) -> str:
        """Suffix explaining a likely-auth-related GitHub API failure.

        GitHub's contents API returns 404 (not 403) for a private repo/path
        when the request is unauthenticated, so an unauthenticated 404 is
        ambiguous between "wrong path" and "needs a token" -- surface both
        possibilities instead of only the first.
        """
        if token or status_code not in (403, 404):
            return ""
        return (
            "  (if this is a private repo, set GITHUB_TOKEN: "
            "export GITHUB_TOKEN=$(gh auth token))"
        )

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
        """Analyze a Pydantic class to extract schema.

        `table=True` SQLModel classes are intentionally NOT skipped here:
        whether a table is expected to come from dbt is business knowledge
        that isn't recoverable from the Python source (two structurally
        identical `table=True` classes can have opposite answers), so it
        can't be inferred from the class definition. Tables that genuinely
        have no corresponding dbt model (e.g. Kafka-populated) are excluded
        via the explicit `mapping.exclude` config instead -- see
        ContractValidator.
        """
        # An explicit __tablename__ is the source of truth for the target name
        # (e.g. `class VideoViewed(SQLModel): __tablename__ = "int_unified_video_viewed"`).
        # Only the class-name heuristic is used when it's absent.
        table_name = self._get_tablename(node) or self._class_to_table_name(node.name)

        columns = []

        # Parse type annotations
        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                field_name = item.target.id
                field_type = self._parse_type_annotation(item.annotation)
                is_required = not self._is_optional_type(item.annotation)

                columns.append(
                    self._make_column(
                        field_name,
                        raw_type=field_type,
                        canonical_type=self._python_to_canonical(field_type),
                        required=is_required,
                        nullable=not is_required,
                    )
                )

        if not columns:
            return None

        # Create source identifier
        if file_source:
            source = f"pydantic:{node.name}@{file_source}"
        else:
            source = f"pydantic:{node.name}"

        # Pydantic models are an authoritative, fully-parsed declaration.
        return Schema(
            name=table_name,
            columns=columns,
            source=source,
            metadata={"confidence": "high", "complete": True},
        )

    def _get_tablename(self, node: ast.ClassDef) -> Optional[str]:
        """Return the value of an explicit `__tablename__ = "..."`, if present."""
        for item in node.body:
            targets = None
            value = None
            if isinstance(item, ast.Assign):
                targets = item.targets
                value = item.value
            elif isinstance(item, ast.AnnAssign) and item.value is not None:
                targets = [item.target]
                value = item.value

            if not targets:
                continue

            for target in targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "__tablename__"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                ):
                    return value.value

        return None

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
