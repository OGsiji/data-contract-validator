# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/OGsiji/retl_validator/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/OGsiji/retl_validator/releases/tag/v1.0.0