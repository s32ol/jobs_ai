#!/usr/bin/env bash

set -u

commit_message="${1:-chore: quick update}"

current_branch="$(git branch --show-current 2>/dev/null)"
if [ -z "${current_branch}" ]; then
  echo "Could not determine the current branch. Make sure you are on a named branch before using quick_push.sh."
  exit 1
fi

echo "Current branch: ${current_branch}"
echo
echo "Git status:"
git status --short --branch
status_exit=$?
if [ "${status_exit}" -ne 0 ]; then
  echo
  echo "Unable to read git status. Make sure this directory is a healthy git repository."
  exit "${status_exit}"
fi

echo
echo "Staging all changes..."
git add --all
add_exit=$?
if [ "${add_exit}" -ne 0 ]; then
  echo "Staging failed. Resolve the git issue above and try again."
  exit "${add_exit}"
fi

if git diff --cached --quiet; then
  echo
  echo "There is nothing new to commit. Everything is already committed, so I am stopping here."
  exit 0
fi

echo
echo "Creating commit:"
echo "  ${commit_message}"
git commit -m "${commit_message}"
commit_exit=$?
if [ "${commit_exit}" -ne 0 ]; then
  echo
  echo "Commit failed. Check your git configuration, hooks, or staged changes, then try again."
  exit "${commit_exit}"
fi

commit_hash="$(git rev-parse --short HEAD 2>/dev/null)"

echo
echo "Pushing to origin/${current_branch}..."
git push origin "${current_branch}"
push_exit=$?
if [ "${push_exit}" -ne 0 ]; then
  echo
  echo "Push failed. Check your remote name, branch permissions, and authentication for origin, then retry."
  exit "${push_exit}"
fi

echo
echo "Quick push complete."
echo "Branch: ${current_branch}"
echo "Commit: ${commit_hash}"
echo "Remote: origin/${current_branch}"
