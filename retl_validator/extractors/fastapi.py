import ast
import inspect
import importlib
from typing import Dict, List, Set, Any, Optional, get_type_hints, Union
from pydantic import BaseModel
from pydantic.fields import FieldInfo


class FastAPIExtractor:
    """
    FastAPI/Pydantic Schema Extractor - Based on your test_ci.py PydanticSchemaExtractor
    """

    def __init__(self):
        self.extracted_schemas = {}

    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract schema requirements from Pydantic models - Your existing logic
        """

        models_module = config.get("models_module")
        if not models_module:
            raise ValueError("models_module is required for FastAPI extraction")

        try:
            module = importlib.import_module(models_module)

            # Find all Pydantic models in the module
            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and issubclass(obj, BaseModel)
                    and obj != BaseModel
                ):

                    print(f"   🔍 Found Pydantic model: {name}")
                    schema_info = self._analyze_model(obj)
                    if schema_info:
                        table_name = schema_info["table_name"]
                        self.extracted_schemas[table_name] = schema_info
                        print(f"   ✅ Added {name} -> {table_name}")
                    else:
                        print(f"   ⚠️  Skipped {name} (no table mapping)")

            return self.extracted_schemas

        except ImportError as e:
            raise ImportError(f"Could not import models module '{models_module}': {e}")

    def _analyze_model(self, model_class: type) -> Optional[Dict[str, Any]]:
        """Analyze a single Pydantic model - your existing logic"""

        # Skip models that don't represent database entities
        if not hasattr(model_class, "__annotations__"):
            return None

        # Try to infer table name from model name or Config
        table_name = self._infer_table_name(model_class)
        if not table_name:
            return None

        schema_info = {
            "table_name": table_name,
            "model_class": model_class.__name__,
            "columns": [],
            "required_columns": [],
            "optional_columns": [],
        }

        # Get field information
        model_fields = getattr(model_class, "model_fields", {})
        type_hints = get_type_hints(model_class)

        for field_name, field_info in model_fields.items():
            column_info = self._analyze_field(
                field_name, field_info, type_hints.get(field_name)
            )
            schema_info["columns"].append(column_info)

            if column_info["required"]:
                schema_info["required_columns"].append(field_name)
            else:
                schema_info["optional_columns"].append(field_name)

        return schema_info

    def _infer_table_name(self, model_class: type) -> Optional[str]:
        """Infer database table name from Pydantic model - your existing logic"""

        # Check if model has explicit table name configuration
        if hasattr(model_class, "Config") and hasattr(model_class.Config, "table_name"):
            return model_class.Config.table_name

        # Check for common naming patterns
        class_name = model_class.__name__

        # Skip abstract/base models
        if "Base" in class_name or "Abstract" in class_name:
            return None

        # Convert CamelCase to snake_case for table name
        import re

        table_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", class_name)
        table_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", table_name).lower()

        # Add common table naming patterns
        if not table_name.endswith("s") and not table_name.endswith("_data"):
            table_name += "s"  # Pluralize

        return table_name

    def _analyze_field(
        self, field_name: str, field_info: FieldInfo, type_hint: Any
    ) -> Dict[str, Any]:
        """Analyze a single Pydantic field - your existing logic"""

        # Check if field is required
        is_required = self._is_field_required(field_info, type_hint)

        column_info = {
            "name": field_name,
            "required": is_required,
            "type": self._python_to_sql_type(type_hint),
            "nullable": not is_required,
        }

        # Check for field constraints
        if hasattr(field_info, "max_length") and field_info.max_length:
            column_info["max_length"] = field_info.max_length

        return column_info

    def _is_field_required(self, field_info: FieldInfo, type_hint: Any) -> bool:
        """Determine if a field is required - your existing logic"""

        # Method 1: Check the type hint for Optional
        type_str = str(type_hint)
        if "typing.Optional" in type_str or "Optional[" in type_str:
            return False

        # Method 2: Check for Union with None (which is what Optional becomes)
        if hasattr(type_hint, "__origin__") and type_hint.__origin__ is Union:
            if type(None) in type_hint.__args__:
                return False

        # Method 3: Check if Field(...) was used (Ellipsis means required)
        if hasattr(field_info, "default") and field_info.default is ...:
            return True

        # Method 5: Check if field has default_factory
        if (
            hasattr(field_info, "default_factory")
            and field_info.default_factory is not None
        ):
            return False

        # Default to required if we can't determine otherwise
        return True

    def _python_to_sql_type(self, python_type: Any) -> str:
        """Convert Python type hints to SQL types - your existing logic"""

        type_str = str(python_type).lower()

        # Handle Optional types
        if "optional" in type_str or "union" in type_str:
            # Extract the non-None type
            if hasattr(python_type, "__args__"):
                for arg in python_type.__args__:
                    if arg != type(None):
                        return self._python_to_sql_type(arg)

        # Basic type mappings
        if "str" in type_str:
            return "varchar"
        elif "int" in type_str:
            return "integer"
        elif "float" in type_str:
            return "float"
        elif "bool" in type_str:
            return "boolean"
        elif "datetime" in type_str:
            return "timestamp"
        elif "date" in type_str:
            return "date"
        elif "dict" in type_str or "json" in type_str:
            return "json"
        elif "list" in type_str:
            return "json"
        else:
            return "varchar"
