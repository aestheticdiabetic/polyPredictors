#!/usr/bin/env bash
# Config tamper guard - blocks edits to linter/formatter configs
# PreToolUse hook

set -euo pipefail

INPUT=$(cat)

FILE=$(echo "$INPUT" | python -c "
import sys, json
d = json.load(sys.stdin)
p = d.get('tool_input', {}).get('file_path', '') or d.get('tool_input', {}).get('path', '')
print(p)
" 2>/dev/null || echo "")

PROTECTED=(
    "pyproject.toml"
    ".ruff.toml"
    "ruff.toml"
    "setup.cfg"
    ".flake8"
)

BASENAME=$(basename "$FILE")

for cfg in "${PROTECTED[@]}"; do
    if [[ "$BASENAME" == "$cfg" ]]; then
        echo "[plankton] Blocked: editing linter config '$BASENAME' is not allowed to suppress violations. Fix the code instead."
        exit 2
    fi
done

exit 0
