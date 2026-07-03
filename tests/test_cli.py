"""
Tests for CLI helper functions.
"""

from unittest.mock import patch, Mock

from data_contract_validator.cli import _github_path_exists, _github_auth_hint


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
