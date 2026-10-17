# Contributing to Fraud Atlas

Thank you for your interest in contributing to Fraud Atlas. This guide explains how to set up the project locally, run tests, and submit improvements.

## How to contribute

1. Fork the repository.
2. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/my-improvement
   ```
3. Make your changes.
4. Run tests locally.
5. Open a pull request describing the change.

## Local development setup

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # or
   source .venv/bin/activate  # macOS/Linux
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the app locally:
   ```bash
   python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
   ```
4. Open `http://localhost:8000` in your browser.

## Testing

Run the test suite before submitting changes:

```bash
pytest -q
```

## Code style and quality

- Keep Python code readable and consistent with the existing style.
- Use descriptive variable and function names.
- Add tests for new behavior when possible.
- Document any API or architecture changes in `README.md`.

## GitHub Actions

This repository has a CI workflow defined at `.github/workflows/python-app.yml`. It runs on push and pull requests to `main` and executes:

- `pip install -r requirements.txt`
- `pytest -q`

## Reporting issues

If you find a bug or want to request a feature, please open an issue with a clear description and reproduction steps.
