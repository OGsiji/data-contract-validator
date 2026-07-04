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

    @patch("requests.get")
    def test_from_github_repo_with_ref_passes_ref_as_query_param(self, mock_get):
        """A branch/tag/commit ref lets someone validate a dev branch's API
        models against dbt instead of always reading the default branch."""
        import base64

        content = "test content"
        encoded_content = base64.b64encode(content.encode()).decode()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"content": encoded_content}
        mock_get.return_value = mock_response

        extractor = FastAPIExtractor.from_github_repo(
            "org/repo", "path/to/file.py", ref="dev"
        )

        assert extractor.content == content
        assert extractor.source == "github:org/repo/path/to/file.py@dev"
        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {"ref": "dev"}

    @patch("requests.get")
    def test_from_github_repo_without_ref_sends_no_ref_param(self, mock_get):
        """No ref means GitHub's default: the repo's default branch."""
        import base64

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": base64.b64encode(b"x").decode()
        }
        mock_get.return_value = mock_response

        FastAPIExtractor.from_github_repo("org/repo", "path/to/file.py")

        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {}

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

    def test_table_true_classes_are_extracted_not_skipped(self):
        """`table=True` alone carries no information about whether a source
        (dbt) model is expected to exist for a table -- two structurally
        identical `table=True` classes can need opposite treatment (one
        genuinely has no dbt model because it's Kafka-populated, another is
        a normal dbt-fed table someone also uses as their API response
        shape). So `table=True` must NOT cause a class to be skipped at
        extraction time; the extractor has no way to make that call
        correctly. Tables that truly have no source model are excluded via
        the validator's `mapping.exclude`, an explicit human decision, not
        an inferred one."""
        content = """
from sqlmodel import SQLModel, Field

class CreatorAudienceDemographics(SQLModel, table=True):
    __tablename__ = "creator_audience_demographics"
    creator_id: str = Field(primary_key=True)
    fan_count: int
"""
        extractor = FastAPIExtractor(content, source="test")
        schemas = extractor.extract_schemas()

        assert "creator_audience_demographics" in schemas
        columns = [c["name"] for c in schemas["creator_audience_demographics"].columns]
        assert "creator_id" in columns
        assert "fan_count" in columns
