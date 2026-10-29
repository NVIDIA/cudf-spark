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

"""Reject replacement-rule metadata constructor lambdas.

Replacement rules must refer to named metadata factories.
"""

import argparse
import collections
import os
import re
import sys


IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
Token = collections.namedtuple("Token", "value start end line")
Violation = collections.namedtuple("Violation", "path line factories")


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
    return offset


def _consume_quoted(source, start):
    quote = source[start]
    if quote == '"' and source.startswith('"""', start):
        end = source.find('"""', start + 3)
        return len(source) if end < 0 else end + 3
    offset = start + 1
    while offset < len(source):
        if source[offset] == "\\":
            offset += 2
        elif source[offset] == quote:
            return offset + 1
        elif quote == "'" and source[offset] in "\r\n":
            return start + 1
        else:
            offset += 1
    return len(source)


def tokenize(source):
    """Tokenize the Scala syntax needed by this lint, hiding comments and literals."""
    tokens = []
    offset = 0
    line = 1
    while offset < len(source):
        if source.startswith("//", offset):
            end = source.find("\n", offset + 2)
            offset = len(source) if end < 0 else end
        elif source.startswith("/*", offset):
            end = _consume_block_comment(source, offset)
            line += source.count("\n", offset, end)
            offset = end
        elif source[offset] in "\"'":
            end = _consume_quoted(source, offset)
            line += source.count("\n", offset, end)
            offset = end
        elif source[offset].isspace():
            if source[offset] == "\n":
                line += 1
            offset += 1
        else:
            match = IDENTIFIER.match(source, offset)
            if match:
                end = match.end()
                tokens.append(Token(match.group(0), offset, end, line))
                offset = end
            elif source.startswith("=>", offset):
                tokens.append(Token("=>", offset, offset + 2, line))
                offset += 2
            else:
                tokens.append(Token(source[offset], offset, offset + 1, line))
                offset += 1
    return tokens


OPEN_TO_CLOSE = {"(": ")", "[": "]", "{": "}"}
CLOSE_TO_OPEN = {value: key for key, value in OPEN_TO_CLOSE.items()}


def _matching_token(tokens, start):
    expected = OPEN_TO_CLOSE[tokens[start].value]
    stack = [expected]
    for index in range(start + 1, len(tokens)):
        value = tokens[index].value
        if value in OPEN_TO_CLOSE:
            stack.append(OPEN_TO_CLOSE[value])
        elif value in CLOSE_TO_OPEN:
            if value != stack[-1]:
                return None
            stack.pop()
            if not stack:
                return index
    return None


def _has_four_parameters(tokens, open_index, close_index):
    stack = []
    commas = 0
    has_content = False
    for token in tokens[open_index + 1:close_index]:
        value = token.value
        if value in OPEN_TO_CLOSE:
            stack.append(OPEN_TO_CLOSE[value])
        elif value in CLOSE_TO_OPEN:
            if not stack or value != stack[-1]:
                return False
            stack.pop()
        elif value == "," and not stack:
            commas += 1
        else:
            has_content = True
    return has_content and commas == 3 and not stack


def _lambda_body_end(tokens, start):
    if start >= len(tokens):
        return start
    if tokens[start].value == "{":
        close = _matching_token(tokens, start)
        return len(tokens) if close is None else close + 1

    stack = []
    statement_starters = {
        "class", "def", "enum", "export", "given", "import", "object", "package",
        "trait", "type", "val", "var",
    }
    for index in range(start, len(tokens)):
        value = tokens[index].value
        if (index > start and not stack and tokens[index].line > tokens[index - 1].line and
                value in statement_starters):
            return index
        if value in OPEN_TO_CLOSE:
            stack.append(OPEN_TO_CLOSE[value])
        elif value in CLOSE_TO_OPEN:
            if not stack:
                return index
            if value != stack[-1]:
                return index
            stack.pop()
        elif value in (",", ";") and not stack:
            return index
    return len(tokens)


def _metadata_type(tokens, new_index, end):
    index = new_index + 1
    names = []
    while index < end:
        value = tokens[index].value
        if IDENTIFIER.fullmatch(value):
            names.append(value)
            index += 1
            if index < end and tokens[index].value == ".":
                index += 1
                continue
            break
        return None
    if not names or not names[-1].endswith("Meta"):
        return None
    if index < end and tokens[index].value == "[":
        close = _matching_token(tokens, index)
        if close is None or close >= end:
            return None
        index = close + 1
    if index >= end or tokens[index].value != "(":
        return None
    return ".".join(names)


def scan_source(path, source):
    tokens = tokenize(source)
    violations = []
    for index, token in enumerate(tokens):
        if token.value != "(":
            continue
        close = _matching_token(tokens, index)
        if close is None or close + 1 >= len(tokens) or tokens[close + 1].value != "=>":
            continue
        if not _has_four_parameters(tokens, index, close):
            continue
        body_start = close + 2
        body_end = _lambda_body_end(tokens, body_start)
        factories = []
        body_index = body_start
        while body_index < body_end:
            nested_body_start = None
            if tokens[body_index].value == "(":
                nested_close = _matching_token(tokens, body_index)
                if (nested_close is not None and nested_close + 1 < body_end and
                        tokens[nested_close + 1].value == "=>"):
                    nested_body_start = nested_close + 2
            elif (IDENTIFIER.fullmatch(tokens[body_index].value) and
                  body_index + 1 < body_end and tokens[body_index + 1].value == "=>"):
                nested_body_start = body_index + 2
            if nested_body_start is not None:
                body_index = _lambda_body_end(tokens, nested_body_start)
                continue
            if tokens[body_index].value == "new":
                metadata_type = _metadata_type(tokens, body_index, body_end)
                if metadata_type and metadata_type not in factories:
                    factories.append(metadata_type)
            body_index += 1
        if factories:
            violations.append(Violation(path, token.line, tuple(factories)))
    return tuple(violations)


def scan_tree(root):
    violations = []
    for current_root, dirs, files in os.walk(root):
        dirs[:] = sorted(name for name in dirs if name not in (".git", "target"))
        normalized_root = current_root.replace(os.sep, "/")
        if "/src/main" not in normalized_root:
            continue
        for filename in sorted(files):
            if not filename.endswith(".scala"):
                continue
            absolute = os.path.join(current_root, filename)
            relative = os.path.relpath(absolute, root).replace(os.sep, "/")
            with open(absolute, encoding="utf-8") as source_file:
                violations.extend(scan_source(relative, source_file.read()))
    return tuple(sorted(violations, key=lambda item: (item.path, item.line, item.factories)))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    args = parser.parse_args(argv)

    root = os.path.abspath(args.root)
    violations = scan_tree(root)
    if violations:
        print("Replacement-rule metadata constructor lambdas:", file=sys.stderr)
        for violation in violations:
            print("  {0}:{1}: use a named metadata factory instead of {2}".format(
                violation.path, violation.line, ", ".join(violation.factories)),
                file=sys.stderr)
        return 1

    print("Replacement-rule metadata lambda lint passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
