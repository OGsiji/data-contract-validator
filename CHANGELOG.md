# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/OGsiji/data-contract-validator/compare/v1.0.5...HEAD
[1.0.5]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.0.5
[1.0.0]: https://github.com/OGsiji/data-contract-validator/releases/tag/v1.0.0