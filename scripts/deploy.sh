#!/usr/bin/env bash
# Deploy the medallion pipeline bundle to Databricks (dev target).
# Run from the repository root (parent of this scripts folder).
# Prerequisites: Databricks CLI installed and authenticated.
#   Install: https://docs.databricks.com/dev-tools/cli/index.html

set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v databricks &>/dev/null; then
  echo "Databricks CLI not found. Install it first: https://docs.databricks.com/dev-tools/cli/index.html" >&2
  exit 1
fi

echo "Validating bundle..."
databricks bundle validate

echo "Deploying to target: dev"
databricks bundle deploy -t dev
