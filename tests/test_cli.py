"""
Tests for CLI helper functions.
"""

from unittest.mock import patch, Mock

import pytest
import yaml
from click.testing import CliRunner

from data_contract_validator.cli import (
    cli,
    _github_path_exists,
    _github_auth_hint,
    _create_github_workflow,
    _detect_branch_hint,
    _resolve_github_ref,
    _build_target_extractor,
)
from data_contract_validator.extractors.hubspot import HubSpotExtractor


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

    @patch("data_contract_validator.cli.requests.get")
    def test_sends_ref_as_query_param_when_provided(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        _github_path_exists("org/repo", "app/models", ref="dev")

        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {"ref": "dev"}

    @patch("data_contract_validator.cli.requests.get")
    def test_no_ref_param_when_omitted(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        _github_path_exists("org/repo", "app/models")

        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {}


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
            input="\ny\n\n\n\n\n\nn\n",
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
        # ("github"), repo (org/repo), path within repo (default), ref
        # (blank = repo's default branch), disable_manifest, then "no" to
        # the pre-commit hook question.
        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\n\n\ngithub\nmy-org/my-api\n\n\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load((tmp_path / ".retl-validator.yml").read_text())
        assert config["target"]["fastapi"] == {
            "type": "github",
            "repo": "my-org/my-api",
            "path": "app/models",
        }

    @patch("data_contract_validator.cli._github_path_exists", return_value=None)
    def test_github_choice_with_ref_includes_it_in_config(
        self, mock_exists, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)

        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\n\n\ngithub\nmy-org/my-api\n\ndev\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load((tmp_path / ".retl-validator.yml").read_text())
        assert config["target"]["fastapi"] == {
            "type": "github",
            "repo": "my-org/my-api",
            "path": "app/models",
            "ref": "dev",
        }
        args, _ = mock_exists.call_args
        assert args[0] == "my-org/my-api"
        assert args[1] == "app/models"
        assert args[3] == "dev"


class TestInitOffersPrecommitSetup:
    """`init --interactive` used to require a separate `setup-precommit`
    invocation for a pre-commit hook. It now offers to set one up as part of
    the same wizard, so people who want both don't need two commands."""

    def _init_local_input(self, extra: str) -> str:
        # dbt path, continue-anyway, target-kind (default api), framework,
        # local-or-github (default local), models location (default),
        # disable_manifest, then whatever pre-commit answers the test gives.
        return "\ny\n\n\n\n\n\n" + extra

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
    repo. It's still the default, since it works as-is for the common case
    (a public target repo) with zero extra setup -- but the generated
    workflow carries a hard-to-miss recommendation to switch to a
    user-created PAT secret for a private target, rather than requiring
    that setup unconditionally."""

    def test_github_target_defaults_to_github_token_with_private_repo_warning(
        self, tmp_path
    ):
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

        assert "GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}" in content
        assert "API_REPO_TOKEN" in content
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


class TestDetectBranchHint:
    """Auto-detecting "the branch this run is about", so a dbt change headed
    for `dev` checks against the API repo's `dev` branch without anyone
    hand-wiring a ref."""

    def test_prefers_pr_base_ref_over_everything(self, tmp_path, monkeypatch):
        # In a PR, the branch being merged INTO is the environment this
        # change is heading toward -- not the feature branch it's on.
        monkeypatch.setenv("GITHUB_BASE_REF", "dev")
        monkeypatch.setenv("GITHUB_REF_NAME", "feature/my-change")
        assert _detect_branch_hint(str(tmp_path)) == "dev"

    def test_falls_back_to_ref_name_for_plain_push(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
        monkeypatch.setenv("GITHUB_REF_NAME", "dev")
        assert _detect_branch_hint(str(tmp_path)) == "dev"

    def test_uses_local_git_branch_outside_ci(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
        monkeypatch.delenv("GITHUB_REF_NAME", raising=False)

        import subprocess

        subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
        subprocess.run(
            ["git", "-C", str(tmp_path), "checkout", "-q", "-b", "dev"], check=True
        )

        assert _detect_branch_hint(str(tmp_path)) == "dev"

    def test_returns_none_when_nothing_detectable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
        monkeypatch.delenv("GITHUB_REF_NAME", raising=False)
        # tmp_path is not a git repo -> no branch to detect.
        assert _detect_branch_hint(str(tmp_path)) is None


class TestResolveGithubRef:
    """An explicit ref always wins; otherwise match the branch on the target
    repo, and fall back to its default branch rather than erroring."""

    def test_explicit_ref_wins_without_any_lookup(self, tmp_path):
        with patch("data_contract_validator.cli._github_path_exists") as mock_exists:
            resolved = _resolve_github_ref(
                "v2.0", "org/api", "app/models", None, str(tmp_path)
            )
        assert resolved == "v2.0"
        mock_exists.assert_not_called()

    @patch("data_contract_validator.cli._github_path_exists", return_value=True)
    def test_matches_branch_on_target_when_it_exists(
        self, mock_exists, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("GITHUB_BASE_REF", "dev")
        resolved = _resolve_github_ref(
            None, "org/api", "app/models", None, str(tmp_path)
        )
        assert resolved == "dev"

    @patch("data_contract_validator.cli._github_path_exists", return_value=False)
    def test_falls_back_to_default_branch_when_absent_on_target(
        self, mock_exists, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("GITHUB_BASE_REF", "some-branch-only-in-dbt-repo")
        resolved = _resolve_github_ref(
            None, "org/api", "app/models", None, str(tmp_path)
        )
        assert resolved is None

    def test_no_hint_means_no_ref(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
        monkeypatch.delenv("GITHUB_REF_NAME", raising=False)
        with patch("data_contract_validator.cli._github_path_exists") as mock_exists:
            resolved = _resolve_github_ref(
                None, "org/api", "app/models", None, str(tmp_path)
            )
        assert resolved is None
        mock_exists.assert_not_called()


class TestHubSpotTargetWiring:
    """A HubSpot target is configured like any other target block, but its
    credential deliberately lives in an env var rather than the (committed)
    config file."""

    def test_wizard_produces_hubspot_config_with_scoped_fields(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("HUBSPOT_ACCESS_TOKEN", "pat-test")

        # dbt path, continue-anyway, target-kind "hubspot", object_type,
        # fields, disable_manifest, then decline the pre-commit hook.
        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\nhubspot\ncontacts\nemail, lifecyclestage\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load((tmp_path / ".retl-validator.yml").read_text())
        assert config["target"]["hubspot"] == {
            "type": "hubspot",
            "object_type": "contacts",
            "fields": ["email", "lifecyclestage"],
        }

    def test_wizard_never_writes_the_token_into_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("HUBSPOT_ACCESS_TOKEN", "pat-super-secret")

        result = CliRunner().invoke(
            cli,
            ["init", "--interactive", "--output-dir", str(tmp_path)],
            input="\ny\nhubspot\ncontacts\nemail\n\nn\n",
        )

        assert result.exit_code == 0, result.output
        raw = (tmp_path / ".retl-validator.yml").read_text()
        assert "pat-super-secret" not in raw

    def test_build_extractor_errors_clearly_without_token(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HUBSPOT_ACCESS_TOKEN", raising=False)

        with pytest.raises(ValueError, match="HUBSPOT_ACCESS_TOKEN"):
            _build_target_extractor(
                "hubspot",
                {"type": "hubspot", "object_type": "contacts"},
                str(tmp_path),
            )

    def test_build_extractor_errors_clearly_without_object_type(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HUBSPOT_ACCESS_TOKEN", "pat-test")

        with pytest.raises(ValueError, match="object_type"):
            _build_target_extractor("hubspot", {"type": "hubspot"}, str(tmp_path))

    def test_build_extractor_returns_hubspot_extractor(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HUBSPOT_ACCESS_TOKEN", "pat-test")

        extractor = _build_target_extractor(
            "hubspot",
            {"type": "hubspot", "object_type": "deals", "fields": ["amount"]},
            str(tmp_path),
        )

        assert isinstance(extractor, HubSpotExtractor)
        assert extractor.object_type == "deals"
        assert extractor.fields == {"amount"}

    def test_unknown_target_type_names_the_valid_options(self, tmp_path):
        with pytest.raises(ValueError, match="local.*github.*hubspot"):
            _build_target_extractor(
                "mystery", {"type": "salesforce"}, str(tmp_path)
            )
