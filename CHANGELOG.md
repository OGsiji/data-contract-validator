# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.7] - 2026-07-04

### Changed
- **The generated CI workflow now defaults `GITHUB_TOKEN` to
  `secrets.API_REPO_TOKEN`** — a token you create yourself — instead of the
  auto-provided `secrets.GITHUB_TOKEN`, which only has access to the repo
  the workflow runs in. A personal access token works identically for
  public and private targets, so this removes the silent-failure case
  entirely rather than just documenting it (1.1.6's fix). Still skipped
  entirely for a `local` target.

### Added
- The generated CI workflow now includes a commented scaffold for
  `dbt deps && dbt docs generate`, unlocking Tier 1 (real warehouse types)
  in CI instead of that only being mentioned in prose docs. Commented out
  by default since it needs the user's warehouse adapter and credentials
  filled in, which can't be inferred.

## [1.1.6] - 2026-07-03

### Fixed
- **The generated CI workflow silently assumed the default
  `secrets.GITHUB_TOKEN` could read the target API repo.** That token only
  has access to the repo the workflow itself runs in — if `target.*.repo`
  is a *different*, private repo, validation would fail on every PR with no
  indication why. The generated workflow now documents the fix inline
  (a personal-access-token secret pointed at that specific repo) and skips
  the `GITHUB_TOKEN` env block entirely for a `local` target, which never
  talks to the GitHub API at all.

## [1.1.5] - 2026-07-03

### Fixed
- **Python `int` was mapped to the narrower `INTEGER` canonical rank,
  producing a false "type mismatch" warning against any dbt column typed
  `bigint`** (a very common type for count/id columns). Python's `int` is
  arbitrary-precision, unlike a fixed-width SQL `INTEGER` column, so there's
  no real truncation risk — it's now mapped to the wider `BIGINT` rank.
  A genuinely fractional source (`DECIMAL`/`FLOAT`) is still flagged.

### Added
- `init --interactive` now offers to set up a pre-commit hook as part of the
  same wizard, instead of requiring a separate `setup-precommit` invocation.
  (The GitHub Actions CI workflow was already created automatically by
  `init` for both the interactive and non-interactive paths — only the
  pre-commit step needed folding in.)

## [1.1.4] - 2026-07-03

### Changed
- **`table=True` SQLModel classes are no longer skipped during extraction.**
  Whether a table is meant to come from dbt is business knowledge that can't
  be recovered from the Python source — two structurally identical
  `table=True` classes can need opposite treatment (one is a normal dbt-fed
  table an API also returns directly; another is populated by a separate
  pipeline like Kafka and was never meant to have a dbt model). Blanket-
  skipping every `table=True` class silently exempted the former case from
  validation too, which is likely the more common pattern — defeating the
  tool's purpose for it. `table=True` classes are now validated like any
  other target.
- Added `mapping.exclude: [<table>, ...]` so the latter case (genuinely no
  source model, e.g. Kafka-populated) can be stated explicitly instead of
  inferred from `table=True`. Excluded tables are skipped entirely and never
  produce a "missing table" issue.

## [1.1.3] - 2026-07-03

Supersedes 1.1.2, which was only ever published to TestPyPI for verification
and never released to production PyPI.

### Fixed
- **`table=True` SQLModel classes were incorrectly evaluated as required API
  contracts.** The standard `class Foo(SQLModel, table=True)` syntax puts
  `table=True` on the class definition's own keywords, not nested inside a
  `Call` base — the skip check only looked in the latter, so DB-only tables
  never matched and produced permanent, unfixable "missing table" criticals.
- **Explicit `__tablename__` is now resolved and used as the target table
  name**, instead of only the class-name-derived guess. A class like
  `VideoViewed` with `__tablename__ = "int_unified_video_viewed"` now matches
  its real source model without needing a manual `mapping.tables` entry.
- **`init --interactive` no longer guesses local vs. GitHub from the path's
  shape.** A local relative path like `app/models` (the wizard's own
  suggested default) is syntactically identical to a GitHub `org/repo`
  string, and was always guessed as a repo, producing a nonsensical
  `app/models/app/models` GitHub target. The wizard now asks explicitly
  ("local project or a different GitHub repo?") before asking for the
  path, and asks for the repo and the path within it as separate prompts.

### Added
- `init --interactive` and `contract-validator test` now verify a configured
  GitHub target path actually exists via the GitHub API, instead of silently
  accepting a stale or typo'd path.
- GitHub API error messages hint at setting `GITHUB_TOKEN` when an
  unauthenticated 404 is ambiguous with a private repo.

### Changed
- **`contract-validator init` no longer silently overwrites an existing
  `.retl-validator.yml` or generated workflow file.** Re-running `init` (e.g.
  after upgrading to pick up a newer version's config defaults) now refuses
  and exits if either file already exists — pass `--force` to regenerate
  them from scratch. Previously this was an unconditional overwrite with no
  confirmation, which could silently destroy hand-added `mapping` entries.

## [1.1.1] - 2026-06-30

### Added
- **Automatic plural/singular table & column matching.** dbt models are
  conventionally plural (`users`) while Pydantic classes are singular
  (`User` → `user`); these now match automatically with no `mapping` needed.
  Candidate forms are only matched against names that actually exist on the
  other side, so it never over-strips (`address` is never mistaken for
  `addres`). Explicit `mapping` still takes precedence.

## [1.1.0] - 2026-06-30

This release is focused on **accuracy** — making a red check always mean a real
problem and a green check genuinely safe, so the tool can be trusted to gate a
deploy.

### Added
- **Canonical type system** (`core/types.py`): every extractor now normalizes
  its native types (warehouse SQL types, Python hints) into a shared, neutral
  vocabulary (`CanonicalType`). The validator compares canonical types instead
  of raw strings, eliminating the bulk of false "type mismatch" warnings
  (e.g. dbt `varchar` vs Pydantic `str` are now correctly equal).
  - Dialect-aware normalization: Snowflake `NUMBER(38,0)`→bigint, BigQuery
    `INT64`/`FLOAT64`, Redshift `SUPER`, Postgres `jsonb`, and more.
- **Tiered dbt extraction** with graceful degradation:
  1. `catalog.json` — real warehouse types (high confidence).
  2. `sqlglot` — a proper SQL parser. Handles CTEs, `||`, window functions, and
     quoted identifiers that the old regex parser mangled. Detects `SELECT *`
     and flags the schema as incomplete.
  3. regex — last-resort best effort (low confidence, never hard-fails).
- **Confidence-aware validation**: when source columns can't be fully resolved
  (e.g. `SELECT *`), a missing column is reported as a **warning, not a
  build-blocking critical**. Type warnings are suppressed for low-confidence
  (regex-tier) sources. This is the core false-positive guard.
- **Explicit mapping config** (`mapping:` in `.retl-validator.yml`) for when
  name heuristics aren't enough — map a target table/column to a differently
  named source model/column:
  ```yaml
  mapping:
    tables:
      user_analytics: user_analytics_summary
    columns:
      user_analytics:
        userId: user_id
  ```
- **Name normalization**: tables/columns now match across snake_case, camelCase
  and casing differences (`userId` == `user_id` == `USER_ID`).

### Changed
- `Schema` now carries `confidence` and `is_complete` (via `metadata`).
- `BaseExtractor` no longer contains Python-specific type mapping; type
  normalization lives in the canonical type system. Added `_make_column` helper.
- Added `sqlglot` as a dependency (imported optionally; falls back to regex if
  absent).

### Fixed
- Hardened GitHub API rate-limit handling against non-dict response headers
  (previously could raise when headers weren't a mapping).

## [1.0.5] - 2025-01-24

### Fixed
- **CRITICAL**: Fixed missing return statement in `DBTExtractor.extract_schemas()` that could return `None` instead of dictionary
  - Added fallback to SQL file parsing when manifest.json is unavailable
  - Now works reliably with or without DBT CLI installed
- **HIGH**: Fixed function signature mismatch in `_test_configuration()` causing TypeError on `--dry-run` command
  - Added missing `disable_manifest` parameter
  - Enhanced to display manifest parsing status
- **MEDIUM**: Replaced bare exception handler in `_try_compile_dbt()` with specific exception types
  - Now properly handles TimeoutExpired, FileNotFoundError
  - Provides helpful error messages instead of silent failures
  - Respects keyboard interrupts
- **MEDIUM**: Removed unused `fastapi_directory` parameter from CLI
  - Simplified API - use `--fastapi-local` for both files and directories
- **MEDIUM**: Added comprehensive YAML error handling with user-friendly messages
  - Catches malformed YAML files with helpful suggestions
  - Validates required configuration sections
  - Provides clear error messages instead of Python tracebacks
- **LOW**: Added GitHub API rate limiting detection and handling
  - Monitors rate limit headers and warns when limits are low
  - Provides helpful guidance to use GITHUB_TOKEN for higher limits
  - Better error messages for 403 and 404 responses

### Improved
- Enhanced error messages throughout the application
- Better support for different use-cases:
  - DBT projects with or without manifest.json
  - Local files and directories for FastAPI models
  - GitHub repositories with rate limit awareness
  - Configuration validation with clear error reporting

## [1.0.0] - 2025-01-XX

### Added
- Initial release of Data Contract Validator
- DBT schema extraction from SQL files and manifest.json
- FastAPI/Pydantic model extraction from local files and GitHub repos
- Command-line interface with multiple output formats
- GitHub Actions integration
- Contract validation with critical/warning/info severity levels
- Support for multiple repositories and complex validation scenarios

### Features
- ✅ DBT model schema extraction
- ✅ FastAPI/Pydantic schema extraction
- ✅ Cross-repository validation
- ✅ GitHub Actions workflows
- ✅ Multiple output formats (terminal, JSON, GitHub Actions)
- ✅ Comprehensive error reporting with suggested fixes
- ✅ Type compatibility checking
- ✅ Missing table/column detection

### Known Limitations
- Only supports DBT and FastAPI currently
- Requires manual installation of DBT CLI
- Limited type inference from SQL
- No support for complex nested types

[Unreleased]: https://github.com/OGsiji/data-contract-validator/compare/v1.1.1...HEAD
[1.1.1]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.1.1
[1.1.0]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.1.0
[1.0.5]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.0.5
[1.0.0]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.0.0