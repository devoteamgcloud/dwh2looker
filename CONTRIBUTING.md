# Contributing to dwh2looker

First off, thank you for considering contributing to `dwh2looker`! It's people like you that make it such a great tool.

We welcome all kinds of contributions, including bug reports, feature requests, documentation improvements, and code changes.

## Setting Up Your Development Environment

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally:
    ```bash
    git clone https://github.com/your-username/dwh2looker.git
    cd dwh2looker
    ```
3.  **Create a virtual environment** (recommended):
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```
4.  **Install the project in editable mode** with development dependencies:
    ```bash
    pip install -e ".[dev]"
    ```
    *Note: If you plan to run tests against a specific database, you might want to install `.[all]` or `.[dev,bigquery]`, etc.*

## Linting and Formatting

We use `ruff` for both linting and formatting.

To check your code for linting errors and formatting issues:
```bash
ruff check .
ruff format --check .
```

To automatically fix formatting and fixable linting errors:
```bash
ruff check --fix .
ruff format .
```

Please ensure your code passes both `ruff check` and `ruff format` before submitting a pull request. Our CI pipeline will automatically check this.

## Running Tests

We use `pytest` for testing. To run the test suite:

```bash
pytest
```

If you add new features or fix bugs, please add corresponding tests.

## Submitting a Pull Request (PR)

1.  **Create a new branch** for your feature or bug fix:
    ```bash
    git checkout -b feature/my-new-feature
    ```
    *Use a descriptive name, like `fix/issue-number-bug-name` or `feature/new-database-support`.*
2.  **Make your changes** and commit them with clear, concise commit messages.
3.  **Ensure tests and linters pass** locally.
4.  **Push your branch** to your fork on GitHub:
    ```bash
    git push origin feature/my-new-feature
    ```
5.  **Open a Pull Request** against the `main` branch of the upstream repository.
6.  Fill out the PR template provided, describing your changes in detail.

## Adding Support for New Databases

Currently, we strongly support Google BigQuery. If you want to add support for Snowflake, Redshift, Postgres, or others:
1.  Check `src/dwh2looker/db_client/db_client.py`.
2.  You will need to implement a new client class that inherits from the base class or implements the necessary interface to extract schema information.
3.  Ensure you add the necessary dependencies to `pyproject.toml` as optional dependencies.

Thank you for contributing!
