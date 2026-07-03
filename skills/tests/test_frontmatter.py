# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for skill frontmatter headers.
"""

from pathlib import Path

import pytest
import yaml

SKILLS_DIR = Path(__file__).resolve().parents[1]
SKILL_FILES = sorted(SKILLS_DIR.glob("*/SKILL.md"))


def _read_frontmatter(path: Path) -> str:
    """Return the YAML frontmatter body from a SKILL.md file."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        raise ValueError(f"{path} must start with YAML frontmatter")

    try:
        end = lines.index("---", 1)
    except ValueError as e:
        raise ValueError(f"{path} must close YAML frontmatter with ---") from e

    return "\n".join(lines[1:end])


@pytest.mark.parametrize("skill_file", SKILL_FILES, ids=lambda p: p.parent.name)
def test_skill_frontmatter_loads(skill_file: Path) -> None:
    frontmatter = _read_frontmatter(skill_file)
    parsed = yaml.safe_load(frontmatter)

    assert isinstance(parsed, dict), f"{skill_file} frontmatter must parse to a map"
    name = parsed.get("name")
    description = parsed.get("description")
    assert (
        isinstance(name, str) and name.strip()
    ), f"{skill_file} must define a non-empty name"
    assert (
        isinstance(description, str) and description.strip()
    ), f"{skill_file} must define a non-empty description"
