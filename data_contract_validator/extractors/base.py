"""
Base extractor interface for schema extraction.

Every extractor produces a ``Dict[str, Schema]``. Each column it emits should
carry a ``canonical_type`` (a :class:`CanonicalType` value) alongside its raw
native ``type`` string, so the validator can compare apples to apples without
knowing anything about the source framework.

Type normalization lives in :mod:`data_contract_validator.core.types`, not
here -- the base class stays framework-neutral and does not assume Python, SQL,
or any particular dialect.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from ..core.models import Schema
from ..core.types import (
    CanonicalType,
    normalize_name,
    normalize_python_type,
    normalize_sql_type,
)


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

    # -- Helpers for subclasses -------------------------------------------

    @staticmethod
    def _make_column(
        name: str,
        *,
        raw_type: Optional[str] = None,
        canonical_type: Optional[CanonicalType] = None,
        required: bool = True,
        nullable: bool = False,
    ) -> Dict[str, Any]:
        """Build a normalized column dict, including its canonical type.

        Either pass an already-resolved ``canonical_type`` or a ``raw_type``
        string that will be normalized as a SQL/warehouse type. Subclasses that
        work with non-SQL native types (e.g. Python hints) should resolve the
        canonical type themselves and pass it in.
        """
        if canonical_type is None:
            canonical_type = normalize_sql_type(raw_type)
        return {
            "name": name,
            "type": raw_type if raw_type is not None else canonical_type.value,
            "canonical_type": canonical_type.value,
            "required": required,
            "nullable": nullable,
        }

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """Normalize column names for comparison (camelCase/snake_case folded)."""
        return normalize_name(name)

    # Kept for backwards compatibility with any custom subclasses that relied on
    # it; new code should use the canonical type system directly.
    @staticmethod
    def _python_to_canonical(python_type: str) -> CanonicalType:
        """Convert a Python type-hint string to a canonical type."""
        return normalize_python_type(python_type)
