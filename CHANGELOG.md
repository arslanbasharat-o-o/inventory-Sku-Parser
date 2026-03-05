# Changelog

All notable changes to this project are documented in this file.

## [3.0.0] - 2026-03-06
### Added
- OpenAI Responses API structured-output parser integration via `ParsedSKUResult` schema.
- Hybrid rule-first parsing service with retry/failsafe behavior and local parse cache.
- FastAPI live analyzer endpoints for single title and batch parsing.
- Comprehensive validation framework for parser quality, confidence, and performance testing.
- CI/CD pipelines for Python backend and Next.js frontend.

### Changed
- Single-title analyzer UI integrated into the existing dashboard flow.
- Parser response payload expanded with primary/secondary part semantics and correction metadata.
- Docker backend runtime image hardened for stable CI/CD builds.
- Learned runtime artifacts reset to curated baseline for cleaner repository defaults.

### Fixed
- Pipeline reliability issues caused by conflicting dependencies and missing compile checks.
- Frontend proxy route for live analyzer API.
- End-to-end parser contract consistency for structured responses.
