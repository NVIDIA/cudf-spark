#!/usr/bin/env python3

# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prevent new deeply nested withResource scopes in production Scala code."""

from __future__ import annotations

import argparse
import collections
import dataclasses
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_MAX_DEPTH = 4
BASELINE_VERSION = 1
DEFAULT_TRACKING_ISSUE = "https://github.com/NVIDIA/cudf-spark/issues/11713"
ALLOW_DIRECTIVE = "with-resource-lint: allow-deep-nesting"
ALLOW_PATTERN = re.compile(
    r"//\s*with-resource-lint:\s*allow-deep-nesting\s*--\s*(\S.*)$")
ISSUE_PATTERN = re.compile(
    r"(?<!\w)(?:https://github\.com/NVIDIA/cudf-spark/issues/|#)\d+\b")


@dataclasses.dataclass(frozen=True)
class Token:
    value: str
    start: int
    end: int
    line: int


@dataclasses.dataclass(frozen=True)
class LineComment:
    text: str
    start: int
    end: int
    line: int


@dataclasses.dataclass(frozen=True)
class ResourceCall:
    line: int
    fingerprint: str
    resource: str
    exempt: bool


@dataclasses.dataclass(frozen=True)
class Violation:
    path: str
    line: int
    depth: int
    fingerprint: str
    resource: str

    @property
    def baseline_key(self) -> tuple[str, str]:
        return (self.path, self.fingerprint)


@dataclasses.dataclass(frozen=True)
class ScanResult:
    violations: tuple[Violation, ...]
    directive_errors: tuple[str, ...]


def _consume_quoted(source: str, start: int, quote: str) -> int:
    """Return the first offset after a quoted Scala string or character literal."""
    if quote == '"' and source.startswith('"""', start):
        end = source.find('"""', start + 3)
        return len(source) if end < 0 else end + 3

    offset = start + 1
    while offset < len(source):
        char = source[offset]
        if char == "\\":
            offset += 2
        elif char == quote:
            return offset + 1
        elif char in "\r\n" and quote == "'":
            # This is probably a Scala 2 symbol literal rather than a character literal.
            return start + 1
        else:
            offset += 1
    return len(source)


def _is_interpolated_quote(source: str, quote_start: int) -> bool:
    if quote_start == 0:
        return False
    offset = quote_start - 1
    if not (source[offset].isalnum() or source[offset] in "_$"):
        return False
    while offset > 0 and (source[offset - 1].isalnum() or source[offset - 1] in "_$"):
        offset -= 1
    return source[offset].isalpha() or source[offset] in "_$"


def _consume_block_comment(source: str, start: int) -> tuple[int, bool]:
    depth = 1
    offset = start + 2
    while offset < len(source) and depth:
        if source.startswith("/*", offset):
            depth += 1
            offset += 2
        elif source.startswith("*/", offset):
            depth -= 1
            offset += 2
        else:
            offset += 1
    return offset, depth == 0


def _matching_interpolation_brace(source: str, open_brace: int) -> int:
    depth = 1
    offset = open_brace + 1
    while offset < len(source):
        if source.startswith("//", offset):
            newline = source.find("\n", offset + 2)
            offset = len(source) if newline < 0 else newline
        elif source.startswith("/*", offset):
            offset, _ = _consume_block_comment(source, offset)
        elif source[offset] in "\"'":
            if source[offset] == '"' and _is_interpolated_quote(source, offset):
                offset, _ = _consume_interpolated(source, offset)
            else:
                offset = _consume_quoted(source, offset, source[offset])
        elif source[offset] == "{":
            depth += 1
            offset += 1
        elif source[offset] == "}":
            depth -= 1
            if depth == 0:
                return offset
            offset += 1
        else:
            offset += 1
    return len(source)


def _consume_interpolated(source: str, start: int) -> tuple[int, list[tuple[int, int]]]:
    delimiter = '\"\"\"' if source.startswith('\"\"\"', start) else '"'
    offset = start + len(delimiter)
    expressions: list[tuple[int, int]] = []

    while offset < len(source):
        if source.startswith(delimiter, offset):
            return offset + len(delimiter), expressions
        if delimiter == '"' and source[offset] == "\\":
            offset += 2
        elif source.startswith("$$", offset):
            offset += 2
        elif source.startswith("${", offset):
            open_brace = offset + 1
            close_brace = _matching_interpolation_brace(source, open_brace)
            expressions.append((open_brace, close_brace))
            offset = close_brace + 1
        else:
            offset += 1
    return len(source), expressions


def _tokenize(source: str) -> tuple[list[Token], list[LineComment]]:
    """Tokenize enough Scala syntax to match calls and lexical blocks.

    Comments and literal contents are deliberately opaque. This avoids counting braces or
    withResource text embedded in comments and literal text. Executable `${...}` expressions
    inside interpolated strings are tokenized recursively.
    """
    tokens: list[Token] = []
    line_comments: list[LineComment] = []
    offset = 0
    line = 1
    length = len(source)

    while offset < length:
        char = source[offset]
        next_char = source[offset + 1] if offset + 1 < length else ""

        if char.isspace():
            if char == "\n":
                line += 1
            offset += 1
        elif char == "/" and next_char == "/":
            newline = source.find("\n", offset + 2)
            end = length if newline < 0 else newline
            line_comments.append(LineComment(source[offset:end], offset, end, line))
            offset = end
        elif char == "/" and next_char == "*":
            comment_start = offset
            offset, closed = _consume_block_comment(source, offset)
            line += source[comment_start:offset].count("\n")
            if not closed:
                raise ValueError(f"unterminated block comment at offset {comment_start}")
        elif char in "\"'":
            expressions: list[tuple[int, int]] = []
            if char == '"' and _is_interpolated_quote(source, offset):
                end, expressions = _consume_interpolated(source, offset)
            else:
                end = _consume_quoted(source, offset, char)
            literal = source[offset:end]
            tokens.append(Token("<literal>", offset, end, line))
            for open_brace, close_brace in expressions:
                open_line = line + source[offset:open_brace].count("\n")
                tokens.append(Token("{", open_brace, open_brace + 1, open_line))
                expression_start = open_brace + 1
                nested_tokens, nested_comments = _tokenize(
                    source[expression_start:close_brace])
                for token in nested_tokens:
                    tokens.append(Token(
                        token.value,
                        token.start + expression_start,
                        token.end + expression_start,
                        token.line + open_line - 1))
                for comment in nested_comments:
                    line_comments.append(LineComment(
                        comment.text,
                        comment.start + expression_start,
                        comment.end + expression_start,
                        comment.line + open_line - 1))
                close_line = line + source[offset:close_brace].count("\n")
                tokens.append(Token("}", close_brace, close_brace + 1, close_line))
            line += literal.count("\n")
            offset = end
        elif char.isalpha() or char in "_$":
            end = offset + 1
            while end < length and (source[end].isalnum() or source[end] in "_$"):
                end += 1
            tokens.append(Token(source[offset:end], offset, end, line))
            offset = end
        elif char.isdigit():
            end = offset + 1
            while end < length and (source[end].isalnum() or source[end] in "._"):
                end += 1
            tokens.append(Token(source[offset:end], offset, end, line))
            offset = end
        else:
            tokens.append(Token(char, offset, offset + 1, line))
            offset += 1

    return tokens, line_comments


def tokenize(source: str) -> list[Token]:
    return _tokenize(source)[0]


def _matching_delimiter(
    tokens: Sequence[Token],
    open_index: int,
    open_value: str,
    close_value: str,
) -> int | None:
    depth = 0
    for index in range(open_index, len(tokens)):
        value = tokens[index].value
        if value == open_value:
            depth += 1
        elif value == close_value:
            depth -= 1
            if depth == 0:
                return index
    return None


def _canonical_call(tokens: Sequence[Token], start: int, end: int) -> str:
    return "".join(token.value for token in tokens[start:end + 1])


def _directive_lines(
    source: str,
    path: str,
    tokens: Sequence[Token],
    line_comments: Sequence[LineComment],
) -> tuple[set[int], list[str]]:
    exempt_lines: set[int] = set()
    errors: list[str] = []
    lines = source.splitlines()

    for comment in line_comments:
        if ALLOW_DIRECTIVE not in comment.text:
            continue
        match = ALLOW_PATTERN.search(comment.text)
        line_number = comment.line
        if match is None or len(match.group(1).strip()) < 10:
            errors.append(
                f"{path}:{line_number}: {ALLOW_DIRECTIVE} requires a reason of at least "
                "10 characters after ' -- '")
            continue
        if ISSUE_PATTERN.search(match.group(1)) is None:
            errors.append(
                f"{path}:{line_number}: {ALLOW_DIRECTIVE} reason must reference an "
                "NVIDIA/cudf-spark GitHub issue by URL or #number")
            continue

        # The directive applies to a withResource call on the same line or the next nonblank line.
        target = line_number
        has_call_before_comment = any(
            token.value == "withResource" and token.line == line_number and
            token.start < comment.start
            for token in tokens)
        if not has_call_before_comment:
            target += 1
            while target <= len(lines) and not lines[target - 1].strip():
                target += 1
        exempt_lines.add(target)

    return exempt_lines, errors


def scan_source(path: str, source: str, max_depth: int) -> ScanResult:
    try:
        tokens, line_comments = _tokenize(source)
    except ValueError as error:
        return ScanResult((), (f"{path}: {error}",))

    exempt_lines, directive_errors = _directive_lines(
        source, path, tokens, line_comments)
    resource_blocks: dict[int, ResourceCall] = {}

    for index, token in enumerate(tokens):
        if token.value != "withResource" or index + 1 >= len(tokens):
            continue
        argument_open_index = index + 1
        if tokens[argument_open_index].value == "[":
            type_args_close = _matching_delimiter(
                tokens, argument_open_index, "[", "]")
            if type_args_close is None or type_args_close + 1 >= len(tokens):
                continue
            argument_open_index = type_args_close + 1
        if tokens[argument_open_index].value != "(":
            continue
        close_index = _matching_delimiter(tokens, argument_open_index, "(", ")")
        if close_index is None or close_index + 1 >= len(tokens):
            continue
        scope_open = tokens[close_index + 1].value
        if scope_open not in {"{", "("}:
            continue

        canonical = _canonical_call(tokens, index, close_index)
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]
        resource_blocks[close_index + 1] = ResourceCall(
            line=token.line,
            fingerprint=fingerprint,
            resource=canonical,
            exempt=token.line in exempt_lines)

    violations: list[Violation] = []
    scope_stack: list[ResourceCall | None] = []
    for index, token in enumerate(tokens):
        if token.value in {"{", "("}:
            resource_call = resource_blocks.get(index)
            scope_stack.append(resource_call)
            if resource_call is not None:
                resource_ancestors = [call for call in scope_stack if call is not None]
                depth = len(resource_ancestors)
                exempt = any(call.exempt for call in resource_ancestors)
                if depth > max_depth and not exempt:
                    violations.append(Violation(
                        path=path,
                        line=resource_call.line,
                        depth=depth,
                        fingerprint=resource_call.fingerprint,
                        resource=resource_call.resource))
        elif token.value in {"}", ")"} and scope_stack:
            scope_stack.pop()

    return ScanResult(tuple(violations), tuple(directive_errors))


def production_scala_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.scala"):
        relative = path.relative_to(root)
        parts = relative.parts
        if "target" in parts or (parts and parts[0] == "scala2.13"):
            continue
        if any(parts[index:index + 2] == ("src", "main")
               for index in range(len(parts) - 1)):
            yield path


def scan_tree(root: Path, max_depth: int) -> ScanResult:
    violations: list[Violation] = []
    directive_errors: list[str] = []
    for path in sorted(production_scala_files(root)):
        relative = path.relative_to(root).as_posix()
        result = scan_source(relative, path.read_text(encoding="utf-8"), max_depth)
        violations.extend(result.violations)
        directive_errors.extend(result.directive_errors)
    return ScanResult(tuple(violations), tuple(directive_errors))


def load_baseline(path: Path) -> tuple[int, collections.Counter[tuple[str, str]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("version") != BASELINE_VERSION:
        raise ValueError(
            f"unsupported baseline version {data.get('version')}; expected {BASELINE_VERSION}")
    max_depth = data.get("maxDepth")
    if not isinstance(max_depth, int) or max_depth < 1:
        raise ValueError("baseline maxDepth must be a positive integer")
    tracking_issue = data.get("trackingIssue")
    if not isinstance(tracking_issue, str) or ISSUE_PATTERN.fullmatch(tracking_issue) is None:
        raise ValueError("baseline trackingIssue must link to an NVIDIA/cudf-spark GitHub issue")

    entries: collections.Counter[tuple[str, str]] = collections.Counter()
    for entry in data.get("entries", []):
        key = (entry["path"], entry["fingerprint"])
        entries[key] += entry.get("count", 1)
    return max_depth, entries


def baseline_json(violations: Sequence[Violation], max_depth: int) -> str:
    grouped: dict[tuple[str, str], list[Violation]] = collections.defaultdict(list)
    for violation in violations:
        grouped[violation.baseline_key].append(violation)

    entries = []
    for (path, fingerprint), matches in sorted(grouped.items()):
        entry = {
            "path": path,
            "fingerprint": fingerprint,
            "resource": matches[0].resource[:160],
        }
        if len(matches) > 1:
            entry["count"] = len(matches)
        entries.append(entry)

    return json.dumps({
        "version": BASELINE_VERSION,
        "maxDepth": max_depth,
        "trackingIssue": DEFAULT_TRACKING_ISSUE,
        "entries": entries,
    }, indent=2) + "\n"


def new_violations(
    violations: Sequence[Violation],
    baseline: collections.Counter[tuple[str, str]],
) -> list[Violation]:
    remaining = baseline.copy()
    result: list[Violation] = []
    for violation in violations:
        key = violation.baseline_key
        if remaining[key] > 0:
            remaining[key] -= 1
        else:
            result.append(violation)
    return result


def stale_baseline_entries(
    violations: Sequence[Violation],
    baseline: collections.Counter[tuple[str, str]],
) -> collections.Counter[tuple[str, str]]:
    current = collections.Counter(violation.baseline_key for violation in violations)
    return baseline - current


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(),
                        help="repository root (default: current directory)")
    parser.add_argument("--baseline", type=Path,
                        default=Path("scripts/with_resource_nesting_baseline.json"))
    parser.add_argument("--max-depth", type=int, default=None,
                        help="override maximum allowed depth")
    parser.add_argument("--print-baseline", action="store_true",
                        help="print a baseline for the current source tree and exit")
    parser.add_argument("--update-baseline", action="store_true",
                        help="replace the baseline with the current source tree")
    return parser.parse_args(args)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root = args.root.resolve()
    baseline_path = args.baseline
    if not baseline_path.is_absolute():
        baseline_path = root / baseline_path

    baseline: collections.Counter[tuple[str, str]] = collections.Counter()
    baseline_depth = DEFAULT_MAX_DEPTH
    if baseline_path.exists():
        try:
            baseline_depth, baseline = load_baseline(baseline_path)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            print(f"Invalid withResource nesting baseline: {error}", file=sys.stderr)
            return 2

    max_depth = args.max_depth if args.max_depth is not None else baseline_depth
    if max_depth < 1:
        print("--max-depth must be positive", file=sys.stderr)
        return 2

    scan = scan_tree(root, max_depth)
    if scan.directive_errors:
        for error in scan.directive_errors:
            print(error, file=sys.stderr)
        return 1

    generated_baseline = baseline_json(scan.violations, max_depth)
    if args.print_baseline:
        print(generated_baseline, end="")
        return 0
    if args.update_baseline:
        baseline_path.write_text(generated_baseline, encoding="utf-8")
        print(f"Updated {baseline_path} with {len(scan.violations)} violations")
        return 0

    unexpected = new_violations(scan.violations, baseline)
    stale = stale_baseline_entries(scan.violations, baseline)
    if not unexpected and not stale:
        print(
            f"withResource nesting lint passed ({len(scan.violations)} baselined violations, "
            f"maximum allowed depth {max_depth})")
        return 0

    for violation in unexpected:
        resource = violation.resource
        if len(resource) > 120:
            resource = resource[:117] + "..."
        print(
            f"{violation.path}:{violation.line}: withResource nesting depth "
            f"{violation.depth} exceeds {max_depth}\n  resource: {resource}",
            file=sys.stderr)
    if unexpected:
        print(
            f"Found {len(unexpected)} new deep withResource scope(s). Shorten resource lifetimes "
            f"or place '// {ALLOW_DIRECTIVE} -- <reason and issue reference>' immediately before a "
            "scope whose overlap is necessary.",
            file=sys.stderr)
    if unexpected and stale:
        print(
            "New and resolved violations together can mean a baselined call changed text or path. "
            "If review confirms no new deep scope, run with --update-baseline to refresh its "
            "fingerprint.",
            file=sys.stderr)
    if stale:
        stale_count = sum(stale.values())
        print(
            f"The baseline contains {stale_count} resolved violation(s). Run this check with "
            "--update-baseline to ratchet it down.",
            file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
