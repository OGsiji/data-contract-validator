"""
Core data models for validation results and schema definitions.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Any


class IssueSeverity(Enum):
    """Severity levels for validation issues."""

    CRITICAL = "critical"  # Will break API
    WARNING = "warning"  # Might cause issues
    INFO = "info"  # Good to know


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""

    severity: IssueSeverity
    table: str
    column: Optional[str]
    message: str
    category: str = "Unknown"
    suggested_fix: Optional[str] = None
    source_value: Optional[str] = None
    target_value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "severity": self.severity.value,
            "table": self.table,
            "column": self.column,
            "message": self.message,
            "category": self.category,
            "suggested_fix": self.suggested_fix,
            "source_value": self.source_value,
            "target_value": self.target_value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationIssue":
        """Create from dictionary."""
        return cls(
            severity=IssueSeverity(data.get("severity", "warning")),
            table=data.get("table", "Unknown"),
            column=data.get("column"),
            message=data.get("message", ""),
            category=data.get("category", "Unknown"),
            suggested_fix=data.get("suggested_fix"),
            source_value=data.get("source_value"),
            target_value=data.get("target_value"),
        )


@dataclass
class Schema:
    """Represents a table schema."""

    name: str
    columns: List[Dict[str, Any]]
    source: str = "unknown"
    metadata: Optional[Dict[str, Any]] = None

    def get_column(self, name: str) -> Optional[Dict[str, Any]]:
        """Get column by name."""
        for col in self.columns:
            if col.get("name") == name:
                return col
        return None

    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.get("name") for col in self.columns if col.get("name")]


@dataclass
class ValidationResult:
    """Result of contract validation."""

    success: bool
    issues: List[ValidationIssue]
    source_schemas: Dict[str, Schema]
    target_schemas: Dict[str, Schema]
    summary: Optional[str] = None

    @property
    def critical_issues(self) -> List[ValidationIssue]:
        """Get only critical issues."""
        return [i for i in self.issues if i.severity == IssueSeverity.CRITICAL]

    @property
    def warnings(self) -> List[ValidationIssue]:
        """Get only warnings."""
        return [i for i in self.issues if i.severity == IssueSeverity.WARNING]

    @property
    def info_items(self) -> List[ValidationIssue]:
        """Get only info items."""
        return [i for i in self.issues if i.severity == IssueSeverity.INFO]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "summary": self.summary,
            "total_issues": len(self.issues),
            "critical_issues": len(self.critical_issues),
            "warnings": len(self.warnings),
            "info_items": len(self.info_items),
            "issues": [issue.to_dict() for issue in self.issues],
        }
