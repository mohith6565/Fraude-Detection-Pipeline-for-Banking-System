#!/usr/bin/env bash
# Helper: create GitHub repo and push current project using gh CLI
# Usage: ./create_and_push_repo.sh YOUR_USER/REPO_NAME

set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <owner/repo>"
  exit 1
fi
REPO="$1"

# Ensure gh is authenticated
if ! gh auth status >/dev/null 2>&1; then
  echo "Please authenticate with 'gh auth login' first"
  exit 1
fi

echo "Creating repo ${REPO} and pushing current code..."
gh repo create "$REPO" --public --source=. --remote=origin --push

echo "Repository created and pushed. Remember to add secrets: DOCKERHUB_USERNAME, DOCKERHUB_TOKEN, (optional) KUBE_CONFIG_DATA"
