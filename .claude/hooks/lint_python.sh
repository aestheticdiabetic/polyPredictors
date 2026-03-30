#!/usr/bin/env bash
# Plankton-style Python quality gate
# Runs on every Python file edit via PostToolUse hook

set -euo pipefail

# Read the tool input from stdin
INPUT=$(cat)

# Extract file path from tool input JSON
FILE=$(echo "$INPUT" | python -c "
import sys, json
d = json.load(sys.stdin)
p = d.get('tool_input', {}).get('file_path', '') or d.get('tool_input', {}).get('path', '')
print(p)
" 2>/dev/null || echo "")

# Only process Python files
if [[ -z "$FILE" ]] || [[ "$FILE" != *.py ]]; then
    exit 0
fi

# Skip if file doesn't exist
if [[ ! -f "$FILE" ]]; then
    exit 0
fi

# Phase 1: Auto-format (silent)
ruff format --quiet "$FILE" 2>/dev/null || true

# Phase 2: Fix auto-fixable lint violations (silent)
ruff check --fix --quiet "$FILE" 2>/dev/null || true

# Phase 3: Check for remaining violations
VIOLATIONS=$(ruff check --output-format=concise --quiet "$FILE" 2>/dev/null || true)

if [[ -n "$VIOLATIONS" ]]; then
    COUNT=$(echo "$VIOLATIONS" | grep -c "^" || true)
    echo "[plankton] $COUNT violation(s) remain in $FILE:"
    echo "$VIOLATIONS"
    exit 2
fi

exit 0
