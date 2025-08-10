"""
Example of creating custom extractors for other frameworks.
"""

from typing import Dict
from data_contract_validator.extractors.base import BaseExtractor
from data_contract_validator.core.models import Schema


class DjangoExtractor(BaseExtractor):
    """Custom extractor for Django models."""

    def __init__(self, models_module: str):
        self.models_module = models_module

    def extract_schemas(self) -> Dict[str, Schema]:
        """Extract schemas from Django models."""
        import importlib
        import inspect
        from django.db import models

        # Import the Django models module
        module = importlib.import_module(self.models_module)

        schemas = {}

        # Find all Django model classes
        for name, obj in inspect.getmembers(module):
            if (
                inspect.isclass(obj)
                and issubclass(obj, models.Model)
                and obj != models.Model
            ):

                schema = self._analyze_django_model(obj)
                if schema:
                    schemas[schema.name] = schema

        return schemas

    def _analyze_django_model(self, model_class) -> Schema:
        """Analyze a Django model to extract schema."""
        table_name = model_class._meta.db_table

        columns = []
        for field in model_class._meta.fields:
            columns.append(
                {
                    "name": field.name,
                    "type": self._django_to_sql_type(field),
                    "required": not field.null,
                    "nullable": field.null,
                }
            )

        return Schema(name=table_name, columns=columns, source="django")

    def _django_to_sql_type(self, field) -> str:
        """Convert Django field types to SQL types."""
        from django.db import models

        type_mapping = {
            models.CharField: "varchar",
            models.TextField: "text",
            models.IntegerField: "integer",
            models.FloatField: "float",
            models.BooleanField: "boolean",
            models.DateTimeField: "timestamp",
            models.DateField: "date",
            models.JSONField: "json",
        }

        return type_mapping.get(type(field), "varchar")


# Usage example
def example_django_validation():
    """Example using custom Django extractor."""
    from data_contract_validator import ContractValidator
    from data_contract_validator.extractors import DBTExtractor

    # Use built-in DBT extractor with custom Django extractor
    dbt = DBTExtractor(project_path="./dbt-project")
    django = DjangoExtractor(models_module="myapp.models")

    validator = ContractValidator(source=dbt, target=django)
    result = validator.validate()

    print(f"Validation: {'✅ PASSED' if result.success else '❌ FAILED'}")
    return result
