#!/usr/bin/env bash

# Copy sample_app/ from Scylla-K8s-Example (the source of truth) into this repo.
#
#   ./sync_sample_app.bash          copy, then show what changed in sample_app/
#   ./sync_sample_app.bash -n       dry run: list what a copy would change
#   ./sync_sample_app.bash --check  exit 1 if the copy differs from the source (used by the pre-commit hook)
#
# The source defaults to ../Scylla-K8s-Example/sample_app; override with SAMPLE_APP_SRC.
# config/ (TLS material) and any old env file are local to each checkout and never copied.

cd "$(dirname "$0")" || exit 1

src="${SAMPLE_APP_SRC:-../Scylla-K8s-Example/sample_app}"
dst="sample_app"

[[ -d $src ]] || { echo "error: source not found: $src (set SAMPLE_APP_SRC)" >&2; exit 2; }

excludes=(
  --exclude='.git*' --exclude=__pycache__ --exclude=.ruff_cache --exclude=.DS_Store
  --exclude='*.jar' --exclude=target/ --exclude=env --exclude=config/
)

case ${1:-} in
  --check)
    # Compare content, not timestamps: -c checksums, and no -t so mtimes are ignored.
    # Lines starting with '.' are attribute-only changes; anything else is a real difference.
    diffs=$(rsync -rlpc -n -i --delete --copy-unsafe-links "${excludes[@]}" "$src/" "$dst/" | grep -v '^\.')
    [[ -z $diffs ]] && exit 0
    printf '%s/ differs from %s/:\n%s\n' "$dst" "$src" "$diffs"
    exit 1
    ;;
  -n)
    rsync -av -n --delete --copy-unsafe-links "${excludes[@]}" "$src/" "$dst/"
    ;;
  "")
    rsync -a --delete --copy-unsafe-links "${excludes[@]}" "$src/" "$dst/" || exit 1
    git status --short -- "$dst"
    ;;
  *)
    echo "Usage: $0 [-n|--check]" >&2
    exit 2
    ;;
esac
