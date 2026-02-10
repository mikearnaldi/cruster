# Changesets

This project uses [knope](https://knope.tech) with changesets for versioning and releases.

## Creating a Changeset

When making changes that should be released, create a changeset file in `.changeset/`:

```bash
# File: .changeset/<descriptive-name>.md
```

## Changeset Format

```markdown
---
default: patch
---

Short description of the change

Optional longer description with more details.
```

## Package Name

**IMPORTANT:** Always use `default` as the package name, not `cruster`.

This project uses an unnamed single-package configuration in `knope.toml`, which requires the `default` identifier.

```markdown
# Correct
---
default: patch
---

# Incorrect - will cause "No packages are ready to release" error
---
cruster: patch
---
```

## Change Types

| Type    | When to use                                      |
| ------- | ------------------------------------------------ |
| `patch` | Bug fixes, minor improvements, documentation     |
| `minor` | New features, non-breaking API additions         |
| `major` | Breaking changes, API removals or modifications  |

## Examples

### Bug Fix

```markdown
---
default: patch
---

fix: resolve race condition in shard acquisition

Adds proper locking to prevent concurrent shard modifications.
```

### New Feature

```markdown
---
default: minor
---

feat: add support for custom serializers

Entities can now specify custom serialization formats via the `#[serializer]` attribute.
```

### Breaking Change

```markdown
---
default: major
---

breaking: rename `EntityRef` to `EntityHandle`

All references to `EntityRef` must be updated to `EntityHandle`.
```

## Workflow

1. Create changeset file with your changes
2. Commit the changeset with your code changes
3. When merged to `main`, CI automatically:
   - Runs `knope prepare-release` to create a release PR
   - Bumps version based on changeset type
   - Updates CHANGELOG.md
   - Deletes the consumed changeset file
4. Merging the release PR publishes to crates.io
