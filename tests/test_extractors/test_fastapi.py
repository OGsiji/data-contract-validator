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

    def test_explicit_tablename_overrides_class_name_heuristic(self):
        """A SQLModel with __tablename__ should be keyed by that, not the
        class-name-derived guess -- otherwise `VideoViewed` with
        `__tablename__ = "int_unified_video_viewed"` never matches its real
        source table without a manual mapping entry."""
        content = """
from sqlmodel import SQLModel

class VideoViewed(SQLModel):
    __tablename__ = "int_unified_video_viewed"
    video_id: str
    user_id: str
"""
        extractor = FastAPIExtractor(content, source="test")
        schemas = extractor.extract_schemas()

        assert "int_unified_video_viewed" in schemas
        assert "video_viewed" not in schemas

    def test_table_true_via_class_keyword_is_skipped(self):
        """`class Foo(SQLModel, table=True):` puts `table=True` in the class
        definition's keywords, not nested inside a Call base -- this is the
        standard SQLModel syntax and must be recognized as a DB table (and
        therefore skipped), not evaluated as a required API contract."""
        content = """
from sqlmodel import SQLModel, Field

class FeedInteraction(SQLModel, table=True):
    id: str = Field(primary_key=True)
    user_id: str
"""
        extractor = FastAPIExtractor(content, source="test")
        schemas = extractor.extract_schemas()

        assert schemas == {}

    def test_table_true_without_tablename_still_skipped(self):
        """Skip logic must not depend on __tablename__ being present."""
        content = """
from sqlmodel import SQLModel

class AffiliateReward(SQLModel, table=True):
    reward_id: str
"""
        extractor = FastAPIExtractor(content, source="test")
        schemas = extractor.extract_schemas()

        assert schemas == {}
