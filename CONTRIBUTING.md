# Contributing to Kiwi-rs

Thank you for your interest in contributing to Kiwi-rs! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/kiwi-rs.git`
3. Create a new branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `make test`
6. Run linter: `make lint`
7. Format code: `make fmt`
8. Commit your changes: `git commit -am 'feat: add some feature'`
9. Push to the branch: `git push origin feature/your-feature-name`
10. Create a Pull Request

## Development Setup

### Prerequisites

- Rust (stable toolchain)
- Protocol Buffers compiler (protoc)
- Make

### Building

```bash
make build
```

### Testing

```bash
# Run all tests
make test

# Run specific test
cargo test test_name
```

### Code Quality

```bash
# Format code
make fmt

# Check formatting
make fmt-check

# Run clippy
make lint
```

## Commit Message Convention

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks
- `ci:` - CI/CD changes
- `build:` - Build system changes

Examples:
```
feat: add support for Redis streams
fix: resolve memory leak in connection pool
docs: update installation instructions
```

## Pull Request Guidelines

- Keep PRs focused on a single feature or fix
- Update documentation as needed
- Add tests for new features
- Ensure all tests pass
- Follow the existing code style
- Write clear commit messages
- Reference related issues

## Code Style

- Follow Rust standard conventions
- Use `rustfmt` for formatting
- Address all `clippy` warnings
- Write clear, self-documenting code
- Add comments for complex logic

## Testing

- Write unit tests for new functionality
- Add integration tests where appropriate
- Ensure tests are deterministic
- Mock external dependencies

## Documentation

- Update README.md if needed
- Add inline documentation for public APIs
- Include examples for new features
- Keep documentation up-to-date

## Questions?

Feel free to open an issue for any questions or concerns.

Thank you for contributing! 🎉
