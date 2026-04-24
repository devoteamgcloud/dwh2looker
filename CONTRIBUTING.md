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

## Pull Request Process

We use a fully automated release and versioning system. To facilitate this, we require all contributions to follow the **Conventional Commits** standard.

### 1. Prepare Your Branch
*   **Create a new branch** for your feature or bug fix:
    ```bash
    git checkout -b feature/my-new-feature
    ```
    *Use a descriptive name, like `fix/issue-number-bug-name` or `feature/new-database-support`.*
*   **Make your changes** and commit them with clear, concise messages.
*   **Ensure tests and linters pass** locally by running `ruff check .` and `pytest`.

### 2. Submit the Pull Request
*   **Push your branch** to your fork on GitHub.
*   **Open a Pull Request** against the `main` branch.
*   **PR Title:** This is the most critical part. Because we use "Squash and Merge", your PR title becomes the final commit message in the project history and determines the next version number.

#### PR Title Format
`<type>: <description>`

**Common Types:**
* `feat:` A new feature (triggers a **Minor** release)
* `fix:` A bug fix (triggers a **Patch** release)
* `docs:` Documentation only changes
* `chore:` Maintenance tasks, dependency updates, etc.

**Examples:**
* ❌ `Added a new login page`
* ✅ `feat: add new login page`
* ❌ `Fixed the crashing bug`
* ✅ `fix: resolve crash on startup`

*Note: If your PR contains multiple changes, name it after the most significant one (e.g., use `feat:` if it includes both a new feature and a fix).*

### 3. Review and Merge
*   Fill out the PR template provided, describing your changes in detail.
*   Once approved and CI passes, a maintainer will merge your PR.

## Adding Support for New Databases

Currently, we strongly support Google BigQuery. If you want to add support for Snowflake, Redshift, Postgres, or others:
1.  Check `src/dwh2looker/db_client/db_client.py`.
2.  You will need to implement a new client class that inherits from the base class or implements the necessary interface to extract schema information.
3.  Ensure you add the necessary dependencies to `pyproject.toml` as optional dependencies.

Thank you for contributing!
