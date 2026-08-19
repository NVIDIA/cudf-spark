# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared test utilities for JVM skill integration tests.
"""

import re
import subprocess
import sys
from typing import Optional


def _echo_indented(result: subprocess.CompletedProcess, prefix: str = "    ") -> None:
    """Write indented output to stdout/stderr so it is visible via pytest -s."""

    def _indent(text: str) -> str:
        return "\n" + "".join(prefix + line for line in text.splitlines(keepends=True))

    if result.stdout:
        sys.stdout.write(_indent(result.stdout))
    if result.stderr:
        sys.stderr.write(_indent(result.stderr))


def run_mvn(
    work_dir: str,
    *goals: str,
    extra_args: Optional[list[str]] = None,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """Run Maven in work_dir with the given goals."""
    cmd = ["mvn", *goals, "-q"]
    if extra_args:
        cmd.extend(extra_args)
    result = subprocess.run(
        cmd,
        cwd=work_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    _echo_indented(result)
    return result


def run_script(
    script_path: str,
    args: Optional[list[str]] = None,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """Run a bash script with the given arguments."""
    cmd = ["bash", script_path]
    if args:
        cmd.extend(args)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    _echo_indented(result)
    return result


def replace_scala_todo_method(source: str, method_name: str, new_body: str) -> str:
    """
    Replace a TODO method stub in a Scala source file with a real implementation.
    Assumes stubs look like "def foo(...) = ???"
    """
    pattern = re.compile(
        r"  def " + re.escape(method_name) + r"\b[^=]*=\s*\?\?\?",
        re.DOTALL,
    )
    result = pattern.sub(new_body, source, count=1)
    if result == source:
        raise ValueError(f"Could not find TODO method '{method_name}' in source")
    return result
