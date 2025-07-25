"""
Base extractor interface for schema extraction.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from ..core.models import Schema


class BaseExtractor(ABC):
    """Base class for all schema extractors."""

    @abstractmethod
    def extract_schemas(self) -> Dict[str, Schema]:
        """
        Extract schemas from the source.
        
        Returns:
            Dict mapping table names to Schema objects
        """
        pass

    def _python_to_sql_type(self, python_type: str) -> str:
        """Convert Python type hints to SQL types."""
        type_mappings = {
            "str": "varchar",
            "int": "integer", 
            "float": "float",
            "bool": "boolean",
            "datetime": "timestamp",
            "date": "date",
            "list": "json",
            "dict": "json",
        }
        
        # Handle Optional types
        if "optional" in python_type.lower():
            inner_type = python_type.lower().replace("optional[", "").replace("]", "")
            return self._python_to_sql_type(inner_type)
        
        return type_mappings.get(python_type.lower(), "varchar")

    def _normalize_column_name(self, name: str) -> str:
        """Normalize column names for comparison."""
        return name.lower().strip()
