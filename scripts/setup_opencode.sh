#!/bin/bash
# Setup script for OpenCode + OpenRouter integration
# Run this once to install and configure OpenCode for GLM code generation

echo "=== Step 1: Install OpenCode ==="
curl -fsSL https://opencode.ai/install | bash

echo ""
echo "=== Step 2: Verify installation ==="
opencode --version

echo ""
echo "=== Step 3: Configure OpenRouter ==="
echo "You need an OpenRouter API key from https://openrouter.ai/keys"
echo "Once you have it, run:"
echo "  opencode auth login openrouter"
echo ""
echo "Or set the environment variable:"
echo "  export OPENROUTER_API_KEY=your_key_here"

echo ""
echo "=== Step 4: Test GLM ==="
echo "Test with a simple prompt:"
echo "  opencode run --model openrouter/z-ai/glm-5.1 'Write a hello world Python function'"
echo ""
echo "=== Step 5: Start headless server (for reliable file edits) ==="
echo "  opencode serve"
echo "Then from Claude Code, attach and run tasks:"
echo "  opencode run --attach http://localhost:4096 --model openrouter/z-ai/glm-5.1 'your spec here'"

echo ""
echo "=== Done ==="
echo "See CLAUDE.md 'Code Implementation via OpenCode + GLM' for the workflow."
