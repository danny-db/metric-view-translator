#!/usr/bin/env bash
# Convenience runner using uv
# Usage:
#   ./run.sh pytest tests/unit/ -v
#   ./run.sh pytest tests/integration/ -v -m integration
#   ./run.sh python -m dax_translator.cli translate -i model.json
#   ./run.sh python -m dax_translator.cli test-all --create-data

cd "$(dirname "$0")"
exec uv run "$@"
