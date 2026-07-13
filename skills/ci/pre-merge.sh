#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILLS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Activate the gcc-toolset for the native CUDA build.
# shellcheck disable=SC1090
source "/opt/rh/gcc-toolset-${TOOLSET_VERSION:-14}/enable"

print_section() {
    echo ""
    echo "=================================================="
    echo "$1"
    echo "=================================================="
}

handle_error() {
    echo ""
    echo "❌ CI PIPELINE FAILED!"
    echo "Error occurred in: $1"
    echo "Please check the logs above for details."
    exit 1
}

setup_python() {
    print_section "Setting up Python environment"
    python3 -m venv "${SKILLS_DIR}/.venv"
    source "${SKILLS_DIR}/.venv/bin/activate"
    pip install --upgrade pip
    pip install -e "${SKILLS_DIR}[dev]"
    echo "Python: $(python --version)"
}

run_unit_tests() {
    print_section "Running fast tests (pytest -m 'not slow')"
    cd "${SKILLS_DIR}"
    pytest -m "not slow" tests
    echo "✅ Fast tests passed!"
}

run_integration_tests() {
    print_section "Running integration tests (pytest -m slow)"
    cd "${SKILLS_DIR}"
    pytest -m slow -s tests
    echo "✅ Integration tests passed!"
}

main() {
    setup_python           || handle_error "Python setup"
    run_unit_tests         || handle_error "Fast tests"
    run_integration_tests  || handle_error "Integration tests"

    print_section "🎉 All pre-merge checks passed!"
    echo "CI pipeline completed successfully!"
}

main "$@"
