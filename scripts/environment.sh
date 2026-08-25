#!/usr/bin/env bash

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

prepend_path() {
  local directory="$1"
  [[ -d "$directory" ]] || return 0
  case ":${PATH:-}:" in
    *":${directory}:"*) ;;
    *) PATH="${directory}${PATH:+:${PATH}}" ;;
  esac
}

for directory in \
  "${HOME}/.cargo/bin" \
  "${HOME}/.pyenv/bin" \
  "${HOME}/.pyenv/shims" \
  "${HOME}/bin" \
  "/opt/homebrew/sbin" \
  "/opt/homebrew/bin" \
  "${HOME}/.local/bin" \
  "${ROOT}/.venv/bin"
do
  prepend_path "$directory"
done

export PATH
