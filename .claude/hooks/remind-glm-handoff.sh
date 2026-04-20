#!/bin/bash
# Non-blocking reminder (exit 0) — prints to stdout so model sees it
echo "REMINDER: If this is post-plan implementation (>20 lines of code), did you try GLM via OpenCode first?"
echo "Rule: Write spec to .claude/glm_spec.md, then: opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 \"prompt\" -f .claude/glm_spec.md"
echo "If OpenCode already failed once this session, proceed with direct edit."
exit 0
