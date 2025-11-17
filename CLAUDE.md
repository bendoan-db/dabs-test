# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks Asset Bundle (DAB) project for deploying and managing Databricks resources. It uses the `uv` package manager for Python dependency management and Databricks Connect for local development and testing.

## Essential Commands

### Setup and Dependencies
```bash
# Install dependencies (required before running tests or developing locally)
uv sync --dev
```

### Testing
```bash
# Run all tests
uv run pytest

# Run a specific test file
uv run pytest tests/sample_taxis_test.py

# Run a specific test function
uv run pytest tests/sample_taxis_test.py::test_find_all_taxis
```

### Databricks Bundle Operations
```bash
# Deploy to development environment (default target)
databricks bundle deploy --target dev

# Deploy to production environment
databricks bundle deploy --target prod

# Run a job
databricks bundle run

# Validate bundle configuration
databricks bundle validate
```

### Code Formatting
```bash
# Format code with black (configured for 125 character line length)
uv run black .
```

## Architecture

### Bundle Structure
- **databricks.yml**: Main bundle configuration defining deployment targets (dev/prod) with catalog/schema variables
- **resources/**: YAML definitions for Databricks resources (jobs, pipelines, etc.)
- **src/doan_dabs_test/**: Shared Python code for jobs and pipelines
- **tests/**: Unit tests using pytest with Databricks Connect
- **fixtures/**: Test data fixtures (JSON/CSV format)

### Deployment Targets
The project has two deployment targets configured in databricks.yml:

**dev** (default):
- Uses development mode (resources prefixed with `[dev username]`)
- Job schedules are paused by default
- Catalog: `doan`, Schema: `dabs_test_dev`
- Workspace: `https://e2-demo-field-eng.cloud.databricks.com`

**prod**:
- Uses production mode
- Catalog: `doan`, Schema: `dabs_test_prod`
- Explicit root path: `/Workspace/Users/ben.doan@databricks.com/.bundle/${bundle.name}/${bundle.target}`

### Testing Architecture
Tests use Databricks Connect to run against a remote Databricks cluster:
- **conftest.py** provides pytest fixtures including `spark` session and `load_fixture` for loading test data
- Tests automatically fall back to serverless compute if no cluster is configured
- Fixture loader supports both JSON and CSV files from the `fixtures/` directory

### Code Organization
Python code in `src/doan_dabs_test/` can be imported by both:
- Databricks notebooks (via the bundle)
- Unit tests (via pytest)
- Job entry points (via `pyproject.toml` scripts section)

The main entry point (`main.py`) accepts `--catalog` and `--schema` arguments to set the execution context dynamically.

## Key Configuration Details

### Python Dependencies
- Managed via `pyproject.toml` with uv
- Dev dependencies include: pytest, databricks-dlt, databricks-connect~=15.4
- Python version: 3.10-3.13
- Build system: hatchling

### Variable Substitution
Bundle resources can reference variables using `${var.catalog}` and `${var.schema}` syntax, which are resolved based on the deployment target.
