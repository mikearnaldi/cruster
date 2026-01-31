---
paths: '.rulesync/**/*, rulesync.jsonc'
---
# Rules Maintenance

This project uses [rulesync](https://github.com/dyoshikawa/rulesync) to manage AI assistant rule files.

## File Structure

| Directory | Purpose |
|-----------|---------|
| `.rulesync/rules/` | Source rule files (edit these) |
| `.claude/rules/` | Generated files (do not edit directly) |
| `rulesync.jsonc` | Rulesync configuration |

## Regenerating Rules

**After modifying any file in `.rulesync/rules/` or `rulesync.jsonc`, run:**

```bash
corepack pnpm rulesync generate
```

This regenerates the `.claude/rules/` directory from the source files.
