# Contributing

Thank you for considering contributing to CC Codex Crawler!

1. Fork the repository and create a feature branch.
2. Install dependencies and the `pre-commit` tool:
   ```bash
   pip install -r requirements.txt
   pip install pre-commit
   pre-commit install
   ```
3. Run the test suite:
   ```bash
   pytest -q
   ```
4. Open a pull request with a clear description of your changes.

All code should pass `pre-commit` and `pytest` before submission.
