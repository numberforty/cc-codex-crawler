# Contributing

Thank you for considering contributing to **CC Codex Crawler**! Contributions of all kinds are appreciated.

## Issue Templates

When opening an issue on GitHub, please select the appropriate template (bug report or feature request) and include as much relevant information as possible. Clear steps to reproduce and expected behaviour help us resolve issues faster.

## Pull Request Process

1. Fork the repository and create a descriptive feature branch.
2. Install dependencies and set up `pre-commit` hooks:
   ```bash
   pip install -r requirements.txt
   pip install pre-commit
   pre-commit install
   ```
3. Run the test suite locally:
   ```bash
   pytest -q
   ```
4. Commit your changes following the coding style below and open a pull request against `main`. Provide a clear description of the motivation and link related issues when possible.
5. Pull requests are automatically checked with `pre-commit` and `pytest` in CI. Ensure both pass before requesting review.

## Coding Style

The codebase follows [PEP 8](https://peps.python.org/pep-0008/) guidelines and uses the following tools via `pre-commit`:

- [Black](https://github.com/psf/black) for formatting
- [isort](https://github.com/pycqa/isort) for import order
- [Flake8](https://github.com/pycqa/flake8) for linting

Run the following command before submitting your pull request to automatically format and lint your changes:

```bash
pre-commit run --all-files
```

All code must pass `pre-commit` and the test suite before it can be merged.
