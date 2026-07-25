#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
check="$repo_root/scripts/check-release-version.sh"

"$check" v0.11.3

for invalid_tag in 0.11.3 v0.11.2 v0.11.3-rc.1 refs/tags/v0.11.3; do
  if "$check" "$invalid_tag" >/dev/null 2>&1; then
    echo "release version check accepted invalid tag: $invalid_tag" >&2
    exit 1
  fi
done

if "$check" >/dev/null 2>&1; then
  echo "release version check accepted a missing tag" >&2
  exit 1
fi
