"""
Tests for CLI helper functions.
"""

from unittest.mock import patch, Mock

from click.testing import CliRunner

from data_contract_validator.cli import cli, _github_path_exists, _github_auth_hint


class TestGithubPathExists:
    """Test the _github_path_exists helper used by init/test to validate
    a configured target.*.path before the user finds out via a silent
    validation failure."""

    @patch("data_contract_validator.cli.requests.get")
    def test_returns_true_when_path_found(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        assert _github_path_exists("org/repo", "app/models") is True

    @patch("data_contract_validator.cli.requests.get")
    def test_returns_false_when_path_missing(self, mock_get):
        mock_get.return_value = Mock(status_code=404)
        assert _github_path_exists("org/repo", "app/model") is False

    @patch("data_contract_validator.cli.requests.get")
    def test_returns_none_on_network_error(self, mock_get):
        import requests

        mock_get.side_effect = requests.RequestException("boom")
        assert _github_path_exists("org/repo", "app/models") is None

    @patch("data_contract_validator.cli.requests.get")
    def test_sends_auth_header_when_token_provided(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        _github_path_exists("org/repo", "app/models", token="secret")

        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["Authorization"] == "token secret"


class TestGithubAuthHint:
    """A 404 on GitHub's contents API is ambiguous between a wrong path and
    a private repo needing auth -- the hint should only fire in that
    ambiguous case, not when we already know the path was found or a token
    was already used."""

    def test_hints_when_missing_and_no_token(self):
        assert _github_auth_hint(False, None) is not None

    def test_no_hint_when_missing_but_token_present(self):
        assert _github_auth_hint(False, "secret") is None

    def test_no_hint_when_path_found(self):
        assert _github_auth_hint(True, None) is None

    def test_no_hint_when_unverifiable(self):
        assert _github_auth_hint(None, None) is None


class TestInitOverwriteProtection:
    """`.retl-validator.yml` commonly accumulates hand-edits (path fixes,
    mapping.tables entries) -- re-running `init` to pick up a newer version's
    defaults must not silently destroy that without --force."""

    def test_refuses_to_overwrite_existing_config_without_force(self, tmp_path):
        config_file = tmp_path / ".retl-validator.yml"
        config_file.write_text("version: '1.0'\ncustom: hand-edited\n")

        result = CliRunner().invoke(cli, ["init", "--output-dir", str(tmp_path)])

        assert result.exit_code != 0
        assert config_file.read_text() == "version: '1.0'\ncustom: hand-edited\n"

    def test_refuses_to_overwrite_existing_workflow_without_force(self, tmp_path):
        workflow_dir = tmp_path / ".github" / "workflows"
        workflow_dir.mkdir(parents=True)
        workflow_file = workflow_dir / "validate-contracts.yml"
        workflow_file.write_text("custom: hand-edited\n")

        result = CliRunner().invoke(cli, ["init", "--output-dir", str(tmp_path)])

        assert result.exit_code == 0
        assert workflow_file.read_text() == "custom: hand-edited\n"

    def test_force_overwrites_existing_config(self, tmp_path):
        config_file = tmp_path / ".retl-validator.yml"
        config_file.write_text("version: '1.0'\ncustom: hand-edited\n")

        result = CliRunner().invoke(
            cli, ["init", "--output-dir", str(tmp_path), "--force"]
        )

        assert result.exit_code == 0
        assert config_file.read_text() != "version: '1.0'\ncustom: hand-edited\n"
