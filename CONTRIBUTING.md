# Contributing to GERA Framework

Thank you for your interest in contributing to the Governed Enterprise Reconciliation Architecture (GERA) Framework.

## How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Add or update tests as appropriate
5. Run the test suite (`pytest tests/ -v`)
6. Commit your changes (`git commit -m 'Add your feature'`)
7. Push to your branch (`git push origin feature/your-feature`)
8. Open a Pull Request

## Development Setup

```bash
# Clone the repo
git clone https://github.com/andersonqiu/GERA-Framework.git
cd GERA-Framework

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run linter
ruff check gera/
```

## Code Style

- Follow PEP 8 conventions
- Use type hints for all function signatures
- Write docstrings for all public classes and methods
- Keep line length under 99 characters

## Testing

- All new features must include unit tests
- Maintain >90% code coverage
- Tests should be deterministic (use fixed random seeds)

## Reporting Issues

Please use GitHub Issues to report bugs or request features. Include:

- A clear description of the issue
- Steps to reproduce (if applicable)
- Expected vs. actual behavior
- Your Python version and OS

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
