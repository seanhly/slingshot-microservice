#!/usr/bin/env bash
set -euo pipefail

MATURIN_PYPI_TOKEN="$(pass show pypi-token | head -1)"
export MATURIN_PYPI_TOKEN

maturin publish
