#!/usr/bin/env python

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

from __future__ import print_function

import argparse
import collections
import hashlib
import io
import json
import os
import re
import sys


DEFAULT_MAX_DEPTH = 4
BASELINE_VERSION = 1
DEFAULT_TRACKING_ISSUE = "https://github.com/NVIDIA/cudf-spark/issues/11713"
ALLOW_DIRECTIVE = "with-resource-lint: allow-deep-nesting"
ALLOW_PATTERN = re.compile(
    r"//\s*with-resource-lint:\s*allow-deep-nesting\s*--\s*(\S.*)$")
ISSUE_PATTERN = re.compile(
    r"(?<!\w)(?:https://github\.com/NVIDIA/cudf-spark/issues/|#)\d+\b")


try:
    STRING_TYPES = (basestring,)
    TEXT_TYPE = unicode
except NameError:  # Python 3
    STRING_TYPES = (str,)
    TEXT_TYPE = str


Token = collections.namedtuple("Token", "value start end line")
LineComment = collections.namedtuple("LineComment", "text start end line")
ResourceCall = collections.namedtuple(
    "ResourceCall", "line fingerprint resource exempt")


class Violation(collections.namedtuple(
        "ViolationBase", "path line depth fingerprint resource")):
    __slots__ = ()

    @property
    def baseline_key(self):
        return (self.path, self.fingerprint)


ScanResult = collections.namedtuple("ScanResult", "violations directive_errors")
ClassifiedViolation = collections.namedtuple(
    "ClassifiedViolation", "violation status")


def _consume_quoted(source, start, quote):
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


def _is_interpolated_quote(source, quote_start):
    if quote_start == 0:
        return False
    offset = quote_start - 1
    if not (source[offset].isalnum() or source[offset] in "_$"):
        return False
    while offset > 0 and (source[offset - 1].isalnum() or source[offset - 1] in "_$"):
        offset -= 1
    return source[offset].isalpha() or source[offset] in "_$"


def _consume_block_comment(source, start):
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


def _matching_interpolation_brace(source, open_brace):
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


def _consume_interpolated(source, start):
    delimiter = '\"\"\"' if source.startswith('\"\"\"', start) else '"'
    offset = start + len(delimiter)
    expressions = []

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


def _tokenize(source):
    """Tokenize enough Scala syntax to match calls and lexical blocks.

    Comments and literal contents are deliberately opaque. This avoids counting braces or
    withResource text embedded in comments and literal text. Executable `${...}` expressions
    inside interpolated strings are tokenized recursively.
    """
    tokens = []
    line_comments = []
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
                raise ValueError(
                    "unterminated block comment at offset {0}".format(comment_start))
        elif char in "\"'":
            expressions = []
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


def tokenize(source):
    return _tokenize(source)[0]


def _matching_delimiter(tokens, open_index, open_value, close_value):
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


def _canonical_call(tokens, start, end):
    return "".join(token.value for token in tokens[start:end + 1])


def _directive_lines(source, path, tokens, line_comments):
    exempt_lines = set()
    errors = []
    lines = source.splitlines()

    for comment in line_comments:
        if ALLOW_DIRECTIVE not in comment.text:
            continue
        match = ALLOW_PATTERN.search(comment.text)
        line_number = comment.line
        if match is None or len(match.group(1).strip()) < 10:
            errors.append(
                "{0}:{1}: {2} requires a reason of at least "
                "10 characters after ' -- '".format(
                    path, line_number, ALLOW_DIRECTIVE))
            continue
        if ISSUE_PATTERN.search(match.group(1)) is None:
            errors.append(
                "{0}:{1}: {2} reason must reference an NVIDIA/cudf-spark GitHub "
                "issue by URL or #number".format(path, line_number, ALLOW_DIRECTIVE))
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


def scan_source(path, source, max_depth):
    try:
        tokens, line_comments = _tokenize(source)
    except ValueError as error:
        return ScanResult((), ("{0}: {1}".format(path, error),))

    exempt_lines, directive_errors = _directive_lines(
        source, path, tokens, line_comments)
    resource_blocks = {}

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

    violations = []
    scope_stack = []
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


def production_scala_files(root):
    for directory, directory_names, file_names in os.walk(root):
        relative_directory = os.path.relpath(directory, root)
        parts = (() if relative_directory == "." else
                 tuple(relative_directory.split(os.sep)))
        directory_names[:] = sorted(
            name for name in directory_names
            if name != "target" and not (not parts and name == "scala2.13"))
        in_production_source = any(
            parts[index:index + 2] == ("src", "main")
            for index in range(len(parts) - 1))
        if in_production_source:
            for file_name in sorted(file_names):
                if file_name.endswith(".scala"):
                    yield os.path.join(directory, file_name)


def scan_tree(root, max_depth):
    violations = []
    directive_errors = []
    for path in sorted(production_scala_files(root)):
        relative = os.path.relpath(path, root).replace(os.sep, "/")
        with io.open(path, "r", encoding="utf-8") as source_file:
            result = scan_source(relative, source_file.read(), max_depth)
        violations.extend(result.violations)
        directive_errors.extend(result.directive_errors)
    return ScanResult(tuple(violations), tuple(directive_errors))


def _fullmatch(pattern, value):
    match = pattern.match(value)
    return match is not None and match.end() == len(value)


def load_baseline(path):
    with io.open(path, "r", encoding="utf-8") as baseline_file:
        data = json.loads(baseline_file.read())
    if data.get("version") != BASELINE_VERSION:
        raise ValueError(
            "unsupported baseline version {0}; expected {1}".format(
                data.get("version"), BASELINE_VERSION))
    max_depth = data.get("maxDepth")
    if not isinstance(max_depth, int) or max_depth < 1:
        raise ValueError("baseline maxDepth must be a positive integer")
    tracking_issue = data.get("trackingIssue")
    if not isinstance(tracking_issue, STRING_TYPES) or not _fullmatch(
            ISSUE_PATTERN, tracking_issue):
        raise ValueError("baseline trackingIssue must link to an NVIDIA/cudf-spark GitHub issue")

    entries = collections.Counter()
    for entry in data.get("entries", []):
        key = (entry["path"], entry["fingerprint"])
        entries[key] += entry.get("count", 1)
    return max_depth, entries


def baseline_json(violations, max_depth):
    grouped = collections.defaultdict(list)
    for violation in violations:
        grouped[violation.baseline_key].append(violation)

    entries = []
    for (path, fingerprint), matches in sorted(grouped.items()):
        entry = collections.OrderedDict((
            ("path", path),
            ("fingerprint", fingerprint),
            ("resource", matches[0].resource[:160]),
        ))
        if len(matches) > 1:
            entry["count"] = len(matches)
        entries.append(entry)

    baseline = collections.OrderedDict((
        ("version", BASELINE_VERSION),
        ("maxDepth", max_depth),
        ("trackingIssue", DEFAULT_TRACKING_ISSUE),
        ("entries", entries),
    ))
    return TEXT_TYPE(json.dumps(baseline, indent=2, separators=(",", ": "))) + "\n"


def new_violations(violations, baseline):
    remaining = baseline.copy()
    result = []
    for violation in violations:
        key = violation.baseline_key
        if remaining[key] > 0:
            remaining[key] -= 1
        else:
            result.append(violation)
    return result


def stale_baseline_entries(violations, baseline):
    current = collections.Counter(violation.baseline_key for violation in violations)
    return baseline - current


def classify_violations(violations, baseline):
    remaining = baseline.copy()
    classified = []
    for violation in violations:
        key = violation.baseline_key
        status = "baselined" if remaining[key] > 0 else "new"
        if remaining[key] > 0:
            remaining[key] -= 1
        classified.append(ClassifiedViolation(violation, status))
    return classified


def markdown_escape(value):
    return (value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("|", "\\|").replace("\r", " ").replace("\n", " "))


def render_summary(classified, stale, directive_errors, max_depth):
    new_count = sum(1 for item in classified if item.status == "new")
    baselined_count = len(classified) - new_count
    lines = [
        "## withResource nesting audit",
        "",
        ("Found {0} scope(s) deeper than {1}: {2} baselined, {3} new.".format(
            len(classified), max_depth, baselined_count, new_count)),
        "",
    ]
    if classified:
        lines.extend([
            "| Status | Depth | Location | Resource |",
            "| --- | ---: | --- | --- |",
        ])
        for item in classified:
            violation = item.violation
            lines.append("| {0} | {1} | `{2}:{3}` | {4} |".format(
                item.status, violation.depth, markdown_escape(violation.path),
                violation.line, markdown_escape(violation.resource)))
    else:
        lines.append("No deep withResource scopes were found.")

    if stale:
        lines.extend(["", "### Stale baseline entries", ""])
        for (path, fingerprint), count in sorted(stale.items()):
            lines.append("- `{0}` (`{1}`), count {2}".format(
                markdown_escape(path), fingerprint, count))
    if directive_errors:
        lines.extend(["", "### Invalid exemption directives", ""])
        lines.extend("- {0}".format(markdown_escape(error))
                     for error in directive_errors)
    lines.extend([
        "",
        ("Baselined scopes are reported as existing debt and do not fail this check. "
         "New scopes, stale baseline entries, and invalid directives fail the audit."),
        "",
    ])
    return "\n".join(lines)


def command_escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def command_property_escape(value):
    return command_escape(value).replace(":", "%3A").replace(",", "%2C")


def emit_annotations(classified):
    for item in classified[:50]:
        violation = item.violation
        level = "error" if item.status == "new" else "warning"
        message = "depth {0}: {1} ({2})".format(
            violation.depth, violation.resource, item.status)
        print("::{0} file={1},line={2},title=withResource nesting::{3}".format(
            level, command_property_escape(violation.path), violation.line,
            command_escape(message)))
    if len(classified) > 50:
        print("::warning title=withResource nesting::Only 50 of {0} scopes were annotated; "
              "see the job summary and raw report for all findings".format(len(classified)))


def write_raw_report(path, classified, stale, directive_errors, max_depth):
    report = collections.OrderedDict((
        ("version", BASELINE_VERSION),
        ("maxDepth", max_depth),
        ("violations", [collections.OrderedDict((
            ("status", item.status),
            ("path", item.violation.path),
            ("line", item.violation.line),
            ("depth", item.violation.depth),
            ("fingerprint", item.violation.fingerprint),
            ("resource", item.violation.resource),
        )) for item in classified]),
        ("staleBaselineEntries", [collections.OrderedDict((
            ("path", path),
            ("fingerprint", fingerprint),
            ("count", count),
        )) for (path, fingerprint), count in sorted(stale.items())]),
        ("directiveErrors", list(directive_errors)),
    ))
    with io.open(path, "w", encoding="utf-8") as report_file:
        report_file.write(TEXT_TYPE(json.dumps(
            report, indent=2, separators=(",", ": "))) + "\n")


def parse_args(args):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=os.getcwd(),
                        help="repository root (default: current directory)")
    parser.add_argument("--baseline",
                        default="scripts/with_resource_nesting_baseline.json")
    parser.add_argument("--max-depth", type=int, default=None,
                        help="override maximum allowed depth")
    parser.add_argument("--print-baseline", action="store_true",
                        help="print a baseline for the current source tree and exit")
    parser.add_argument("--update-baseline", action="store_true",
                        help="replace the baseline with the current source tree")
    parser.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"),
                        help="write a Markdown report containing every deep scope")
    parser.add_argument("--raw-report",
                        help="write a JSON report containing every deep scope")
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root = os.path.abspath(args.root)
    baseline_path = args.baseline
    if not os.path.isabs(baseline_path):
        baseline_path = os.path.join(root, baseline_path)

    baseline = collections.Counter()
    baseline_depth = DEFAULT_MAX_DEPTH
    if os.path.exists(baseline_path):
        try:
            baseline_depth, baseline = load_baseline(baseline_path)
        except (KeyError, TypeError, ValueError) as error:
            print("Invalid withResource nesting baseline: {0}".format(error),
                  file=sys.stderr)
            return 2

    max_depth = args.max_depth if args.max_depth is not None else baseline_depth
    if max_depth < 1:
        print("--max-depth must be positive", file=sys.stderr)
        return 2

    scan = scan_tree(root, max_depth)

    generated_baseline = baseline_json(scan.violations, max_depth)
    if args.print_baseline:
        print(generated_baseline, end="")
        return 0
    if args.update_baseline:
        with io.open(baseline_path, "w", encoding="utf-8") as baseline_file:
            baseline_file.write(generated_baseline)
        print("Updated {0} with {1} violations".format(
            baseline_path, len(scan.violations)))
        return 0

    unexpected = new_violations(scan.violations, baseline)
    stale = stale_baseline_entries(scan.violations, baseline)
    classified = classify_violations(scan.violations, baseline)
    report_failed = False
    if args.summary:
        try:
            with io.open(args.summary, "w", encoding="utf-8") as summary_file:
                summary_file.write(TEXT_TYPE(render_summary(
                    classified, stale, scan.directive_errors, max_depth)))
        except (IOError, OSError) as error:
            report_failed = True
            print("Could not write withResource summary: {0}".format(error), file=sys.stderr)
    if args.raw_report:
        try:
            write_raw_report(
                args.raw_report, classified, stale, scan.directive_errors, max_depth)
        except (IOError, OSError) as error:
            report_failed = True
            print("Could not write withResource raw report: {0}".format(error), file=sys.stderr)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        emit_annotations(classified)

    if scan.directive_errors:
        for error in scan.directive_errors:
            print(error, file=sys.stderr)
        return 1

    if not unexpected and not stale:
        print(
            "withResource nesting lint passed ({0} baselined violations, maximum "
            "allowed depth {1})".format(len(scan.violations), max_depth))
        return 1 if report_failed else 0

    for violation in unexpected:
        resource = violation.resource
        if len(resource) > 120:
            resource = resource[:117] + "..."
        print(
            "{0}:{1}: withResource nesting depth {2} exceeds {3}\n  resource: {4}".format(
                violation.path, violation.line, violation.depth, max_depth, resource),
            file=sys.stderr)
    if unexpected:
        print(
            "Found {0} new deep withResource scope(s). Shorten resource lifetimes or "
            "place '// {1} -- <reason and issue reference>' immediately before a scope "
            "whose overlap is necessary.".format(len(unexpected), ALLOW_DIRECTIVE),
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
            "The baseline contains {0} resolved violation(s). Run this check with "
            "--update-baseline to ratchet it down.".format(stale_count),
            file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
