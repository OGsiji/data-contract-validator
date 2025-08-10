"""
Pytest configuration and shared fixtures.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_dbt_project(temp_dir):
    """Create a sample DBT project for testing."""
    project_dir = temp_dir / "dbt_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    dbt_config = """
name: 'test_project'
version: '1.0.0'
config-version: 2
model-paths: ["models"]
target-path: "target"
"""
    (project_dir / "dbt_project.yml").write_text(dbt_config)

    # Create models directory
    models_dir = project_dir / "models"
    models_dir.mkdir()

    # Create test model
    test_model = """
select
    id as user_id,
    email,
    first_name,
    last_name,
    created_at
from raw_users
"""
    (models_dir / "users.sql").write_text(test_model)

    return project_dir


@pytest.fixture
def sample_fastapi_content():
    """Sample FastAPI models content."""
    return """
from pydantic import BaseModel
from datetime import datetime

class User(BaseModel):
    user_id: str
    email: str
    first_name: str
    last_name: str
    created_at: datetime
"""


@pytest.fixture
def sample_schemas():
    """Sample schemas for testing validation."""
    return {
        "source": {
            "users": {
                "name": "users",
                "columns": [
                    {"name": "user_id", "type": "varchar", "required": True},
                    {"name": "email", "type": "varchar", "required": True},
                    {"name": "first_name", "type": "varchar", "required": True},
                    {"name": "last_name", "type": "varchar", "required": True},
                    {"name": "created_at", "type": "timestamp", "required": True},
                ],
                "source": "test",
            }
        },
        "target": {
            "user": {
                "name": "user",
                "columns": [
                    {"name": "user_id", "type": "varchar", "required": True},
                    {"name": "email", "type": "varchar", "required": True},
                    {"name": "first_name", "type": "varchar", "required": True},
                    {"name": "last_name", "type": "varchar", "required": True},
                    {"name": "created_at", "type": "timestamp", "required": True},
                ],
                "source": "test",
            }
        },
    }
