from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional

class ValidationSeverity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

@dataclass
class ValidationIssue:
    severity: ValidationSeverity
    table: str
    column: Optional[str]
    message: str
    category: str = "Unknown"
    suggested_fix: Optional[str] = None
    file_path: Optional[str] = None
    fastapi_expectation: Optional[str] = None
    dbt_output: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'severity': self.severity.value,
            'category': self.category,
            'table': self.table,
            'column': self.column,
            'message': self.message,
            'suggested_fix': self.suggested_fix,
            'file_path': self.file_path,
            'fastapi_expectation': self.fastapi_expectation,
            'dbt_output': self.dbt_output
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationIssue':
        """Create from dictionary"""
        return cls(
            severity=ValidationSeverity(data.get('severity', 'error')),
            table=data.get('table', 'Unknown'),
            column=data.get('column'),
            message=data.get('message', ''),
            category=data.get('category', 'Unknown'),
            suggested_fix=data.get('suggested_fix'),
            file_path=data.get('file_path'),
            fastapi_expectation=data.get('fastapi_expectation'),
            dbt_output=data.get('dbt_output')
        )