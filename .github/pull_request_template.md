## SDD Traceability

- Work package: WP-N
- SDD baseline: commit SHA
- Primary issue: Fixes #N / Closes #N / Refs #N / Related #N
- Parent or Epic: Refs #N / Related #N / N/A
- Related issues: Refs #N / Related #N / N/A
- Design context: Discussion #N / N/A
- Requirements:
  - REQ-DOMAIN-NNN
- Decisions:
  - DNNN / N/A

Use Fixes #N or Closes #N only when this PR completely satisfies every required
acceptance criterion of that Issue. Partial work must use Refs #N or Related #N.

## Description

<!-- Describe the problem, design, implementation, and intentionally excluded scope. -->

## Type of Change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring
- [ ] Test or verification infrastructure

## Scope Completion

- [ ] This PR has one primary work-package objective.
- [ ] The primary Issue still matches the current source and PR scope.
- [ ] Fixes is used only for a completely satisfied Issue.
- [ ] Deferred acceptance items are represented by separate open Issues.
- [ ] Required tests and documentation are included.
- [ ] Frozen work such as Embedded Redis Hot Tier is not implemented.

## Verification

- Environment:
- Commands:
- Results:
- Uncovered risks:
- [ ] After merge, record the PR number, merge SHA, exact main commit, commands,
      environment, result, and residual risk in `.planning/SDD.md` before marking
      the work package `verified` or `accepted`.

## Checklist

- [ ] Target branch is main or the approved feature branch.
- [ ] Code follows the Rust style and project conventions.
- [ ] Formatting checks pass.
- [ ] Clippy checks pass.
- [ ] Targeted tests prove the changed success, failure, and boundary paths.
- [ ] Required existing tests pass.
- [ ] Public behavior changes include Redis 8.8.1 Oracle evidence.
- [ ] Storage/Raft changes include required Linux/WSL and fault evidence.
- [ ] Documentation and compatibility manifests are updated.
- [ ] No required test is silently skipped.
- [ ] Security, sensitive logging, and third-party license impact were checked.

## Additional Context

<!-- Add migration, rollback, compatibility, follow-up, or review context. -->
