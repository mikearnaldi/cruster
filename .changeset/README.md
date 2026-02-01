# Changesets

This directory contains changeset files that document changes for the next release.

## Adding a changeset

When you make a change that should be documented in the changelog, create a new file in this directory with a unique name (e.g., `my-change.md`).

The file format is:

```markdown
---
default: minor
---

Description of the change that will appear in the changelog.
```

### Change types

- `major` - Breaking changes
- `minor` - New features (backwards compatible)
- `patch` - Bug fixes (backwards compatible)

## How it works

1. Add changeset files to your PRs
2. Knope Bot creates/updates a "Release X.X.X" PR with all pending changes
3. Merge the release PR to publish

See [knope documentation](https://knope.tech) for more details.
