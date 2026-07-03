# Contributing to Kiwi

Thank you for your interest in contributing to Kiwi!

For development environment setup, build commands, and architecture overview, see [docs/development.md](docs/development.md).

## Pull Request Workflow

1. Fork the repository and clone your fork
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Run `make test` and `make lint` to verify
5. Commit following the [convention](#commit-convention) below
6. Push and create a Pull Request

Keep PRs focused on a single feature or fix. Add tests for new functionality and update documentation as needed.

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/). PR titles are checked against this format by CI.

| Type | Usage |
|------|-------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `refactor` | Code refactoring |
| `perf` | Performance improvement |
| `test` | Adding/updating tests |
| `chore` | Maintenance tasks |
| `ci` | CI/CD changes |
| `build` | Build system changes |
| `style` | Formatting, whitespace |
| `revert` | Revert a previous commit |

Examples:
```
feat: add support for Redis streams
fix: resolve memory leak in connection pool
docs: update installation instructions
```

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
