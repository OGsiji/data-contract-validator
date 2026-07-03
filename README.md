# 🛡️ Data Contract Validator

> **Catch breaking changes between your dbt models and your FastAPI/Pydantic APIs — before they hit production.**

[![PyPI version](https://badge.fury.io/py/data-contract-validator.svg)](https://badge.fury.io/py/data-contract-validator)
[![Tests](https://github.com/OGsiji/data-contract-validator/workflows/Tests/badge.svg)](https://github.com/OGsiji/data-contract-validator/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 What it solves

Your analytics team changes a dbt model. Your API team's FastAPI service still
expects the old shape. Nobody notices until production 500s at 2 AM.

This tool sits on that boundary. It extracts the schema your **dbt models
produce** and the schema your **Pydantic models expect**, compares them, and
fails CI when the data side can no longer satisfy the API side.

```
   dbt models                 Data Contract Validator                FastAPI / Pydantic
(what the pipeline   ──▶   extract → normalize → compare   ◀──   (what the API expects)
    produces)                     ↓
                          critical issues block the build
```

### Built for trust

A check that gates a deploy is only useful if it doesn't cry wolf. v1.1
re-architected extraction around that principle:

- **Canonical types** — dbt `varchar` and Pydantic `str` are understood to be
  the same thing, so you don't get drowned in fake "type mismatch" warnings.
- **A real SQL parser** (`sqlglot`) instead of regex — CTEs, `||`
  concatenation, window functions and quoted identifiers are parsed correctly.
- **Confidence-aware** — if the tool can't fully resolve a model's columns
  (e.g. `SELECT *`), it will **warn** rather than falsely **block** your build.

## ⚡ Quick start

```bash
pip install data-contract-validator
```

```bash
# Initialize config + CI workflow in your dbt project
contract-validator init --interactive

# Sanity-check the setup
contract-validator test

# Validate
contract-validator validate
```

## 🚀 Getting started, step by step

If you're setting this up on a project for the first time, the order below
avoids the sharp edges:

1. **Install into the same environment dbt runs in** (not a separate venv) —
   the tool needs to see your dbt project:
   ```bash
   pip install data-contract-validator
   ```
   Already have `.retl-validator.yml` committed by a teammate? Skip to step 5.

2. **Generate the config + CI workflow** (one-time):
   ```bash
   contract-validator init --interactive
   ```
   You'll be asked: where your dbt project is, which API framework you use,
   whether your models live in this local project or a different GitHub
   repo, and then the local path (or the `org/repo` + path within it). It's
   asked explicitly rather than guessed from the path's shape — a local path
   like `app/models` is syntactically identical to a GitHub `org/repo`
   string, so there's no reliable way to infer which one you mean. If you
   pick GitHub, it checks the path actually exists before writing the
   config — so a typo surfaces here instead of at `validate` time.

   `init` refuses to touch an existing `.retl-validator.yml` or workflow
   file — it won't clobber hand-added `mapping` entries just because you
   upgraded the package and re-ran `init`. Pass `--force` if you really want
   to regenerate them from the new version's defaults.

3. **Pre-commit hook**: `init --interactive` asks whether you want one set
   up right after creating the config and CI workflow — say yes there and
   it's done. To add one later (or if you used non-interactive `init`,
   which doesn't prompt), run it standalone:
   ```bash
   contract-validator setup-precommit --install-hooks
   ```

4. **If the target repo is private, set a token** before running anything
   that talks to GitHub locally:
   ```bash
   export GITHUB_TOKEN=$(gh auth token)   # or a PAT with repo read access
   ```
   See [Private GitHub repos need `GITHUB_TOKEN`](#private-github-repos-need-github_token) below for why this is easy to miss.

5. **Sanity-check the setup**:
   ```bash
   contract-validator test
   ```
   Confirms the config parses, the dbt project is found, and the target
   (local path or GitHub path) is reachable. If this fails, `validate` will
   fail the same way — fix it here first.

6. **Run it**:
   ```bash
   contract-validator validate
   ```

7. **When it reports a critical issue, diagnose before assuming your dbt
   model is wrong**:
   - Real missing column/table → fix the dbt model.
   - Target name doesn't match the dbt model by convention (renamed/prefixed)
     → add an entry under `mapping.tables` in `.retl-validator.yml` (see
     [When do I need `mapping`?](#when-do-i-need-mapping)).
   - A table that's genuinely populated by something other than dbt (e.g. a
     separate streaming pipeline) and has no source model on purpose → add
     it to `mapping.exclude`. `table=True` alone is **not** used to infer
     this automatically — see [FastAPI side](#fastapi-side) for why.

8. **For accurate type-checking** (not just column-presence checks), run
   `dbt docs generate` before `validate` so it picks up `catalog.json` (Tier 1,
   real warehouse types) instead of inferring from SQL text — see
   [How extraction works](#-how-extraction-works-and-why-its-accurate) below.

### One-off validation (no config file)

```bash
# Local dbt project against a local Pydantic models file or directory
contract-validator validate \
  --dbt-project ./my-dbt-project \
  --fastapi-local ./my-api/app/models.py

# dbt project against models in another GitHub repo (microservices)
contract-validator validate \
  --dbt-project . \
  --fastapi-repo "my-org/my-api" \
  --fastapi-path "app/models.py"
```

## 🔍 How extraction works (and why it's accurate)

### dbt side — tiered, best-source-wins

| Tier | Source | Types | Confidence | Notes |
|---|---|---|---|---|
| 1 | `target/catalog.json` | **Real warehouse types** | high | Produced by `dbt docs generate`. Most accurate. |
| 2 | `sqlglot` SQL parse | Inferred (often unknown) | medium | Trusted column **names**; enriched with documented types from `manifest.json`. Detects `SELECT *`. |
| 3 | regex parse | Guessed | low | Last resort. Never used to hard-fail a build. |

The tool auto-detects what's available and degrades gracefully — so it works
offline in pre-commit **and** with full type fidelity in a warehouse-connected
CI job.

> 💡 **Tip:** run `dbt docs generate` in CI before validating to unlock Tier 1
> (real types). Without it, you still get accurate column-presence checks from
> Tier 2. The workflow `init` generates includes this step already, commented
> out — it needs your warehouse adapter and credentials filled in, which
> can't be guessed, so it isn't active by default.

### FastAPI side

Pydantic / SQLModel classes are parsed from source with Python's `ast` (no
imports executed). `Optional[...]` controls whether a field is required.
An explicit `__tablename__` is used as the table name when present;
otherwise the class name is converted to `snake_case`.

`table=True` SQLModel classes are validated the same as any other class —
they are **not** skipped. Whether a table is meant to come from dbt is
business knowledge that isn't recoverable from the Python source: two
structurally identical `table=True` classes can need opposite treatment (one
is a normal dbt-fed table your API also returns directly; another is
populated by a Kafka stream and was never meant to have a dbt model). Use
`mapping.exclude` to state the latter case explicitly rather than relying on
`table=True` to imply it.

## 🚦 What gets flagged

| Severity | Meaning | Example |
|---|---|---|
| 🚨 **Critical** | Blocks the build | API requires a column the dbt model no longer produces |
| ⚠️ **Warning** | Worth a look, non-blocking | A real type mismatch, or a missing column on a model we couldn't fully resolve |

```bash
$ contract-validator validate

🛡️ Data Contract Validation Results:
Status: ❌ FAILED
Critical: 1 | Warnings: 0

🚨 Critical Issues (Must Fix):
  💥 user_analytics
     Column: total_orders
     Problem: Target REQUIRES column 'total_orders' but source doesn't provide it
     🔧 Fix: Add column 'total_orders' to source model for table 'user_analytics'
```

## 🔧 Configuration (`.retl-validator.yml`)

```yaml
version: "1.0"
name: "my-project-contracts"

source:
  dbt:
    project_path: "."
    auto_compile: true
    # Force Tier 2/3 SQL parsing even if catalog/manifest exist:
    disable_manifest: false

target:
  fastapi:
    # GitHub repo:
    type: "github"
    repo: "my-org/my-api"
    path: "app/models.py"
    # ...or local:
    # type: "local"
    # path: "../my-api/app/models.py"

# Optional: explicit mapping for when names don't line up by convention.
mapping:
  tables:
    # target (Pydantic) table : source (dbt) model
    user_analytics: user_analytics_summary
  columns:
    user_analytics:
      # target column : source column
      userId: user_id
  # Target tables with no source model on purpose (e.g. Kafka-populated,
  # not dbt) -- see "When do I need mapping?" below.
  exclude:
    - feed_interaction

validation:
  fail_on: ["missing_tables", "missing_required_columns"]
  warn_on: ["type_mismatches", "missing_optional_columns"]
```

### Private GitHub repos need `GITHUB_TOKEN`

If `target.*.repo` points at a private repository, `contract-validator`
needs a token with read access to it. Where that token comes from is
different locally vs. in CI — and the CI case has a sharp edge worth
understanding before it silently fails on a PR.

**Locally**, set the `GITHUB_TOKEN` environment variable before running the
CLI. On bash/zsh that's `export` (there's nothing to install — `export` just
makes the variable visible to the `contract-validator` process you run
next):

```bash
export GITHUB_TOKEN=$(gh auth token)   # or a PAT with repo read access
contract-validator validate
```

GitHub's API 404s (not 403s) an unauthenticated request to a private path,
so without a token this looks identical to a plain typo in `path` —
`contract-validator init --interactive` and `contract-validator test` both
check `target.*.path` actually exists and will point you at this if the
lookup 404s with no token set.

**In CI**, the workflow `init` generates for a GitHub target wires up
`GITHUB_TOKEN: ${{ secrets.API_REPO_TOKEN }}` — a token **you** create,
*not* the auto-provided `secrets.GITHUB_TOKEN`. That auto-provided token
only has access to **the repository the workflow is running in**, so if
your dbt repo and your API repo are different repos, it silently can't read
the target the first time that target is private — and a PAT works
identically for a public target too, so there's no reason to default to the
token that only sometimes works. To finish the setup the generated workflow
expects:

1. Create a token with read access to the *target* repo — a
   [fine-grained PAT](https://github.com/settings/personal-access-tokens/new)
   scoped to just that repo's Contents (read-only) is the least-privilege
   option; a classic PAT with the `repo` scope also works.
2. In the repo running the workflow (your dbt repo): **Settings → Secrets
   and variables → Actions → New repository secret**. Name it
   **`API_REPO_TOKEN`** exactly (that's the name the generated workflow
   already references) and paste the token as the value.

   > ⚠️ **GitHub rejects any secret name starting with `GITHUB_`** — it's a
   > reserved prefix. You cannot create a secret literally called
   > `GITHUB_TOKEN`; that's not a naming suggestion, the UI will refuse it.
   > That's exactly why the workflow's secret is named `API_REPO_TOKEN`
   > instead, even though the environment variable it feeds is `GITHUB_TOKEN`
   > — two different things with confusingly similar names:
   > ```yaml
   > env:
   >   GITHUB_TOKEN: ${{ secrets.API_REPO_TOKEN }}
   > #  ^^^^^^^^^^^   local variable name, can be anything -- the CLI
   > #                just needs it called GITHUB_TOKEN to find it
   > #                            ^^^^^^^^^^^^^^ the *secret's* name --
   > #                            this is what GitHub restricts
   > ```

Skip all of this for a `local` target — `init` omits the whole `env:` block
since a local target never talks to the GitHub API at all.

### When do I need `mapping`?

Most of the time you don't. Names are matched automatically across:
- `snake_case` / `camelCase` / casing — `UserAnalytics` → `user_analytics`, `userId` → `user_id`
- **plural ↔ singular** — dbt's plural `users` matches Pydantic's `User` (→ `user`)
  with no config (and it won't over-match — `address` is never confused with `addres`).

Reach for `mapping.tables` / `mapping.columns` only when a model or column is
named so differently that convention can't bridge it (e.g. Pydantic
`user_id` ↔ dbt `customer_identifier`).

`mapping.exclude` is different — it's not about renamed models, it's for a
target table that has **no source model on purpose**, because it's
populated by something other than dbt (a Kafka stream, a cron job, etc.).
This can't be inferred from the code (a `table=True` SQLModel class looks
identical whether or not dbt is supposed to feed it), so it has to be a
deliberate, human-stated exception:

```yaml
mapping:
  exclude:
    - feed_interaction
    - affiliate_reward
```

Anything not listed is validated normally — including `table=True` classes,
which are treated the same as any other target and are not silently skipped.

## 🐍 Python API

```python
from data_contract_validator import ContractValidator, DBTExtractor, FastAPIExtractor

dbt = DBTExtractor(project_path="./dbt-project")
fastapi = FastAPIExtractor.from_github_repo("my-org/my-api", "app/models.py")

validator = ContractValidator(
    source_extractor=dbt,
    target_extractor=fastapi,
    mapping={"tables": {"user_analytics": "user_analytics_summary"}},  # optional
)
result = validator.validate()

if not result.success:
    for issue in result.critical_issues:
        print(f"💥 {issue.table}.{issue.column}: {issue.message}")
```

## 🪝 CI / pre-commit integration

### GitHub Actions

`contract-validator init` generates a workflow for you. Minimal version:

```yaml
name: 🛡️ Data Contract Validation
on:
  pull_request:
    paths: ["models/**/*.sql", "dbt_project.yml", "**/*models*.py"]
jobs:
  validate-contracts:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with: { python-version: "3.11" }
      - run: pip install data-contract-validator
      # Optional: `dbt docs generate` here for real warehouse types (Tier 1)
      - run: contract-validator validate --output github
        env:
          GITHUB_TOKEN: ${{ secrets.API_REPO_TOKEN }}
```

`GITHUB_TOKEN` here is only needed if `target` is a `github` repo (`init`
omits the whole `env:` block for a `local` target). `secrets.API_REPO_TOKEN`
is a token you create yourself, not GitHub's auto-provided
`secrets.GITHUB_TOKEN` — see
[Private GitHub repos need `GITHUB_TOKEN`](#private-github-repos-need-github_token)
above for why, and how to set it up.

### Pre-commit

```bash
contract-validator setup-precommit --install-hooks
```

```yaml
repos:
  - repo: https://github.com/OGsiji/data-contract-validator
    rev: v1.1.0
    hooks:
      - id: contract-validation
```

## 🧪 Output formats

```bash
contract-validator validate --output terminal   # human-friendly (default)
contract-validator validate --output json        # machine-readable for CI
contract-validator validate --output github       # GitHub Actions annotations
```

## 🚀 Supported frameworks

**Source:** dbt (all adapters — Snowflake, BigQuery, Redshift, Postgres, …).
**Target:** FastAPI (Pydantic v2 + SQLModel).

The extractor architecture is intentionally pluggable (`BaseExtractor` →
`Dict[str, Schema]` with canonical types), so additional sources/targets can be
added without touching the validator. [Open an issue](https://github.com/OGsiji/data-contract-validator/issues)
to request one.

## 🛠️ Development & testing

```bash
git clone https://github.com/OGsiji/data-contract-validator
cd data-contract-validator

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"     # or: pip install -e ".[test]"

# Run the suite
pytest

# Lint / format
black data_contract_validator tests
```

The test suite covers the canonical type system (`tests/test_core/test_types.py`),
the tiered dbt extractor including sqlglot CTE handling and `catalog.json`
(`tests/test_extractors/test_dbt.py`), and the confidence/mapping behavior of
the validator (`tests/test_core/test_validator.py`).

### Adding an extractor

```python
from data_contract_validator.extractors.base import BaseExtractor
from data_contract_validator.core.types import CanonicalType

class MyExtractor(BaseExtractor):
    def extract_schemas(self):
        # return Dict[str, Schema]; use self._make_column(...) so each column
        # carries a canonical_type the validator can compare.
        ...
```

## 🗺️ Roadmap

- Real compatibility semantics (nullability, additive vs. breaking changes)
- Reporter/logging abstraction (quiet/embeddable core)
- A canonical, language-neutral contract artifact + baseline/snapshot diffing
- More targets (Django, SQLAlchemy, GraphQL, OpenAPI)

## 📄 License

MIT — see [LICENSE](LICENSE).

## 🆘 Support

- 🐛 Issues: https://github.com/OGsiji/data-contract-validator/issues
- 📧 Email: ogunniransiji@gmail.com

If this saves you a production incident, please ⭐ the repo.
