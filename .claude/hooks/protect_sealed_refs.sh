#!/usr/bin/env bash
# PreToolUse guard for sealed reference answers.
#
# Policy: creating a NEW reference file is allowed (/newday needs this).
#         Modifying or overwriting one that ALREADY EXISTS is denied.
#
# Permission rules in settings.json cannot express "only if the file exists",
# which is why this is a hook. Covers Write / Edit / NotebookEdit only —
# writes issued through Bash (`cat > refs/...`) are NOT intercepted here.
#
# Fails CLOSED: if the payload cannot be parsed, anything that looks like a
# reference path is denied rather than waved through.

set -uo pipefail

deny() {
    jq -nc --arg reason "$1" \
        '{hookSpecificOutput: {
             hookEventName: "PreToolUse",
             permissionDecision: "deny",
             permissionDecisionReason: $reason
         }}'
    exit 0
}

input=$(cat)

# Reference material = anything under refs/, or any *_ref.md (legacy day files).
looks_like_ref() {
    case "$1" in
        */refs/*|refs/*|*_ref.md) return 0 ;;
        *) return 1 ;;
    esac
}

if ! command -v jq >/dev/null 2>&1; then
    # No parser available. Deny if the raw payload mentions a reference path.
    if printf '%s' "$input" | grep -qE 'refs/|_ref\.md'; then
        echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"jq unavailable; refusing to touch a possible reference file."}}'
    fi
    exit 0
fi

path=$(printf '%s' "$input" | jq -r '.tool_input.file_path // .tool_input.notebook_path // empty' 2>/dev/null)

if [ -z "$path" ]; then
    # No path field. Only a concern if the payload smells like a ref anyway.
    if printf '%s' "$input" | grep -qE 'refs/|_ref\.md'; then
        deny "Could not read a file path from this call, but it references a sealed reference path. Denied by default."
    fi
    exit 0
fi

looks_like_ref "$path" || exit 0

if [ -e "$path" ]; then
    deny "$(printf '%s already exists and holds sealed reference answers. Creating a NEW reference file is allowed; modifying an existing one is not. If this is genuinely intended, the user must edit or move the file themselves.' "$path")"
fi

exit 0
