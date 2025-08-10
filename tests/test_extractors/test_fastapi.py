"""
Tests for FastAPI extractor.
"""

import pytest
from unittest.mock import patch, Mock

from data_contract_validator.extractors.fastapi import FastAPIExtractor


class TestFastAPIExtractor:
    """Test the FastAPIExtractor class."""

    def test_initialization(self):
        """Test extractor initialization."""
        content = "test content"
        extractor = FastAPIExtractor(content, source="test")

        assert extractor.content == content
        assert extractor.source == "test"

    def test_from_local_file(self, temp_dir, sample_fastapi_content):
        """Test creating extractor from local file."""
        test_file = temp_dir / "models.py"
        test_file.write_text(sample_fastapi_content)

        extractor = FastAPIExtractor.from_local_file(str(test_file))

        assert sample_fastapi_content in extractor.content
        assert str(test_file) in extractor.source

    @patch("requests.get")
    def test_from_github_repo(self, mock_get):
        """Test creating extractor from GitHub repo."""
        import base64

        content = "test content"
        encoded_content = base64.b64encode(content.encode()).decode()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"content": encoded_content}
        mock_get.return_value = mock_response

        extractor = FastAPIExtractor.from_github_repo("org/repo", "path/to/file.py")

        assert extractor.content == content
        assert "github:org/repo/path/to/file.py" == extractor.source

    def test_extract_schemas(self, sample_fastapi_content):
        """Test schema extraction from FastAPI models."""
        extractor = FastAPIExtractor(sample_fastapi_content, source="test")
        schemas = extractor.extract_schemas()

        assert "user" in schemas
        user_schema = schemas["user"]
        assert user_schema.name == "user"
        assert len(user_schema.columns) == 5

        # Check specific columns
        column_names = [col["name"] for col in user_schema.columns]
        assert "user_id" in column_names
        assert "email" in column_names
        assert "created_at" in column_names
