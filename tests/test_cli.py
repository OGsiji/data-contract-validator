"""
Tests for CLI helper functions.
"""

from unittest.mock import patch, Mock

import yaml
from click.testing import CliRunner

from data_contract_validator.cli import (
    cli,
    _github_path_exists,
    _github_auth_hint,
    _create_github_workflow,
)


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


class TestInteractiveSetupAsksLocalOrGithubExplicitly:
    """The wizard asks 'local or GitHub?' explicitly instead of guessing
    from the path's shape. Guessing based on the presence of '/' previously
    misdetected the tool's own suggested default ('app/models') as a GitHub
    'org/repo' string, producing a nonsensical 'app/models/app/models'
    target -- asking up front removes the ambiguity entirely."""

    def test_local_choice_produces_local_config_for_default_path(
        self, tmp_path, monkeypatch
    ):
        (tmp_path / "app" / "models").mkdir(parents=True)
        (tmp_path / "app" / "models" / "user.py").write_text(
            "from pydantic import BaseModel\nclass User(BaseModel):\n    id: str\n"
        )
        monkeypatch.chdir(tmp_path)

        # Prompts: dbt path, continue-anyway (no dbt_project.yml here),
        # framework, local-or-github (default "local"), models location
        # (default "app/models"), disable_manifest, then "no" to the
        # pre-commit hook question (kept out of scope for this test).
        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\n\n\n\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load((tmp_path / ".retl-validator.yml").read_text())
        assert config["target"]["fastapi"] == {"type": "local", "path": "app/models"}

    @patch("data_contract_validator.cli._github_path_exists", return_value=None)
    def test_github_choice_asks_for_repo_and_path_separately(
        self, mock_exists, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)

        # Prompts: dbt path, continue-anyway, framework, local-or-github
        # ("github"), repo (org/repo), path within repo (default),
        # disable_manifest, then "no" to the pre-commit hook question.
        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\n\ngithub\nmy-org/my-api\n\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load((tmp_path / ".retl-validator.yml").read_text())
        assert config["target"]["fastapi"] == {
            "type": "github",
            "repo": "my-org/my-api",
            "path": "app/models",
        }


class TestInitOffersPrecommitSetup:
    """`init --interactive` used to require a separate `setup-precommit`
    invocation for a pre-commit hook. It now offers to set one up as part of
    the same wizard, so people who want both don't need two commands."""

    def _init_local_input(self, extra: str) -> str:
        # dbt path, continue-anyway, framework, local-or-github (default
        # local), models location (default), disable_manifest, then
        # whatever pre-commit answers the test supplies.
        return "\ny\n\n\n\n\n" + extra

    @patch("data_contract_validator.cli._setup_precommit")
    def test_declining_precommit_does_not_create_config(
        self, mock_setup_precommit, tmp_path, monkeypatch
    ):
        (tmp_path / "app" / "models").mkdir(parents=True)
        (tmp_path / "app" / "models" / "user.py").write_text(
            "from pydantic import BaseModel\nclass User(BaseModel):\n    id: str\n"
        )
        monkeypatch.chdir(tmp_path)

        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input=self._init_local_input("n\n"),
        )

        assert result.exit_code == 0, result.output
        mock_setup_precommit.assert_not_called()

    @patch("data_contract_validator.cli._setup_precommit")
    def test_accepting_precommit_calls_setup_with_install_choice(
        self, mock_setup_precommit, tmp_path, monkeypatch
    ):
        (tmp_path / "app" / "models").mkdir(parents=True)
        (tmp_path / "app" / "models" / "user.py").write_text(
            "from pydantic import BaseModel\nclass User(BaseModel):\n    id: str\n"
        )
        monkeypatch.chdir(tmp_path)

        # Accept the hook, decline installing it immediately.
        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input=self._init_local_input("y\nn\n"),
        )

        assert result.exit_code == 0, result.output
        mock_setup_precommit.assert_called_once_with(False)

    def test_non_interactive_init_never_asks_about_precommit(self, tmp_path):
        result = CliRunner().invoke(cli, ["init", "--output-dir", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "pre-commit hook" not in result.output


class TestGeneratedWorkflowGithubToken:
    """The auto-provided secrets.GITHUB_TOKEN only has access to the repo
    the workflow runs in -- it can't read a *different*, private target
    repo. Rather than defaulting to it and documenting the fix, the
    generated workflow defaults straight to a user-created PAT secret,
    which works for both public and private targets, so there's no
    silent-failure case to walk into in the first place."""

    def test_github_target_defaults_to_api_repo_token_secret(self, tmp_path):
        config = {
            "source": {"dbt": {"project_path": "."}},
            "target": {
                "fastapi": {"type": "github", "repo": "org/api", "path": "app/models"}
            },
        }
        _create_github_workflow(tmp_path, config)
        content = (
            tmp_path / ".github" / "workflows" / "validate-contracts.yml"
        ).read_text()

        assert "GITHUB_TOKEN: ${{ secrets.API_REPO_TOKEN }}" in content
        # The auto-provided token is only mentioned in the explanatory
        # comment (as the thing NOT to use) -- it must not appear as an
        # actual directive anywhere.
        assert "${{ secrets.GITHUB_TOKEN }}" not in content
        assert "private" in content.lower()
        assert "New repository secret" in content

    def test_local_target_has_no_token_env_block(self, tmp_path):
        config = {
            "source": {"dbt": {"project_path": "."}},
            "target": {"fastapi": {"type": "local", "path": "app/models"}},
        }
        _create_github_workflow(tmp_path, config)
        content = (
            tmp_path / ".github" / "workflows" / "validate-contracts.yml"
        ).read_text()

        assert "GITHUB_TOKEN" not in content

    def test_generated_workflow_is_valid_yaml(self, tmp_path):
        import yaml

        for target in (
            {"type": "github", "repo": "org/api", "path": "app/models"},
            {"type": "local", "path": "app/models"},
        ):
            config = {
                "source": {"dbt": {"project_path": "."}},
                "target": {"fastapi": target},
            }
            _create_github_workflow(tmp_path, config, force=True)
            content = (
                tmp_path / ".github" / "workflows" / "validate-contracts.yml"
            ).read_text()
            assert yaml.safe_load(content)


class TestGeneratedWorkflowDbtTier1Scaffold:
    """The generated workflow never ran `dbt docs generate`, so CI always
    fell back to Tier 2/3 SQL parsing instead of real warehouse types --
    even though the README's own example implied this was wired up. A
    commented scaffold can't run without the user's warehouse credentials,
    but it should at least be visible in the actual generated file, not
    just mentioned in prose docs."""

    def test_includes_commented_dbt_docs_generate_scaffold(self, tmp_path):
        config = {
            "source": {"dbt": {"project_path": "./dbt-project"}},
            "target": {"fastapi": {"type": "local", "path": "app/models"}},
        }
        _create_github_workflow(tmp_path, config)
        content = (
            tmp_path / ".github" / "workflows" / "validate-contracts.yml"
        ).read_text()

        assert "dbt docs generate" in content
        assert "working-directory: ./dbt-project" in content
        # Must be commented out -- it can't run without real credentials.
        assert "#   run: |\n    #     dbt deps\n    #     dbt docs generate" in content

    def test_scaffold_present_regardless_of_target_type(self, tmp_path):
        for target in (
            {"type": "github", "repo": "org/api", "path": "app/models"},
            {"type": "local", "path": "app/models"},
        ):
            config = {
                "source": {"dbt": {"project_path": "."}},
                "target": {"fastapi": target},
            }
            _create_github_workflow(tmp_path, config, force=True)
            content = (
                tmp_path / ".github" / "workflows" / "validate-contracts.yml"
            ).read_text()
            assert "dbt docs generate" in content
