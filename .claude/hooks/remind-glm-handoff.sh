#!/bin/bash
set +e  # Never fail — this is a non-blocking reminder hook
# Non-blocking reminder (exit 0) — only fires for code files, not markdown/config/brain

# Read tool input JSON from stdin into variable
INPUT="$(cat 2>/dev/null || true)"

# Check if the input contains non-code file extensions — skip silently
if echo "$INPUT" | grep -qE '\.md"|\.json"|\.yaml"|\.yml"|\.toml"|\.txt"|\.csv"|\.sh"|\.env"'; then
  exit 0
fi

# Check if the path is in a non-code directory — skip silently
if echo "$INPUT" | grep -qE '"(.*/(brain|\.claude|handoffs|memory)/)'; then
  exit 0
fi

# For code files (.py, .ts, .tsx, .js, etc.) — exit silently (reminder is in CLAUDE.md)
exit 0
