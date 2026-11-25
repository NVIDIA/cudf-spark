#!/usr/bin/env jython

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

"""Reject function literals passed as replacement-rule metadata factories.

A replacement-rule helper is any def or class with a parameter of the metadata
factory type `(..., DataFromReplacementRule) => ...`. Every call of a helper
must pass a named factory for that parameter, such as a method, a case-class
companion or a curried factory call. Function literals, including
partial-function blocks and placeholder expressions such as
`new FooMeta(_, _, _, _)`, are rejected.
"""

import argparse
import collections
import io
import os
import re
import sys


IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
Token = collections.namedtuple("Token", "value line")
Violation = collections.namedtuple("Violation", "path line helper")
FactoryParam = collections.namedtuple("FactoryParam", "list_index arg_index name")
LITERAL = "<literal>"
FACTORY_TYPE_END = ("DataFromReplacementRule", ")", "=>")
PARAMETER_MODIFIERS = frozenset(
    ["final", "implicit", "override", "private", "protected", "val", "var"])
UNICODE_ARROW = u"\u21d2"  # Scala 2 alternative to =>
OPEN_TO_CLOSE = {"(": ")", "[": "]", "{": "}"}
CLOSE_TO_OPEN = dict((value, key) for key, value in OPEN_TO_CLOSE.items())


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


def _consume_interpolation(source, start):
    """Return the offset after the `}` closing the `${` at start."""
    depth = 0
    offset = start + 1
    while offset < len(source):
        char = source[offset]
        if char == '"':
            offset = _consume_string(source, offset)
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if not depth:
                return offset + 1
        offset += 1
    return offset


def _consume_string(source, start):
    interpolated = start > 0 and (source[start - 1].isalnum() or source[start - 1] == "_")
    triple = source.startswith('"""', start)
    offset = start + (3 if triple else 1)
    while offset < len(source):
        if interpolated and source.startswith("${", offset):
            offset = _consume_interpolation(source, offset)
        elif triple and source.startswith('"""', offset):
            offset += 3
            while offset < len(source) and source[offset] == '"':
                offset += 1
            return offset
        elif not triple and source[offset] == "\\":
            offset += 2
        elif not triple and source[offset] == '"':
            return offset + 1
        else:
            offset += 1
    return len(source)


def _consume_char(source, start):
    """Consume a character literal; a symbol literal or stray quote is one token."""
    if source.startswith("\\", start + 1):
        end = source.find("'", start + 2)
        return start + 1 if end < 0 else end + 1
    if source.startswith("'", start + 2):
        return start + 3
    return start + 1


def tokenize(source):
    """Tokenize the Scala syntax needed by this lint, hiding comments and literals."""
    tokens = []
    offset = 0
    line = 1
    while offset < len(source):
        char = source[offset]
        if source.startswith("//", offset):
            end = source.find("\n", offset)
            offset = len(source) if end < 0 else end
        elif source.startswith("/*", offset):
            end = _consume_block_comment(source, offset)
            line += source.count("\n", offset, end)
            offset = end
        elif char == '"' or char == "'":
            end = (_consume_string if char == '"' else _consume_char)(source, offset)
            tokens.append(Token(LITERAL, line))
            line += source.count("\n", offset, end)
            offset = end
        elif char.isspace():
            if char == "\n":
                line += 1
            offset += 1
        elif source.startswith("=>", offset) or char == UNICODE_ARROW:
            tokens.append(Token("=>", line))
            offset += 1 if char == UNICODE_ARROW else 2
        else:
            match = IDENTIFIER.match(source, offset)
            end = match.end() if match else offset + 1
            tokens.append(Token(source[offset:end], line))
            offset = end
    return tokens


def _matching_token(tokens, start):
    stack = [OPEN_TO_CLOSE[tokens[start].value]]
    index = start + 1
    while index < len(tokens):
        value = tokens[index].value
        if value in OPEN_TO_CLOSE:
            stack.append(OPEN_TO_CLOSE[value])
        elif value in CLOSE_TO_OPEN:
            if value != stack[-1]:
                return None
            stack.pop()
            if not stack:
                return index
        index += 1
    return None


def _argument_lists(tokens, index):
    """Yield the top-level comma-separated (start, end) spans of each argument list."""
    if index < len(tokens) and tokens[index].value == "[":
        close = _matching_token(tokens, index)
        index = len(tokens) if close is None else close + 1
    while index < len(tokens) and tokens[index].value == "(":
        close = _matching_token(tokens, index)
        if close is None:
            return
        spans = []
        start = index + 1
        depth = 0
        for position in xrange(index + 1, close):
            value = tokens[position].value
            if value in OPEN_TO_CLOSE:
                depth += 1
            elif value in CLOSE_TO_OPEN:
                depth -= 1
            elif value == "," and not depth:
                spans.append((start, position))
                start = position + 1
        spans.append((start, close))
        yield spans
        index = close + 1


def _parameter_name(tokens, start, end):
    index = start
    while index < end and (tokens[index].value in PARAMETER_MODIFIERS or
                           tokens[index].value == "@"):
        index += 2 if tokens[index].value == "@" else 1
    if index + 1 < end and tokens[index + 1].value == ":":
        return tokens[index].value
    return None


def find_helpers(token_lists):
    """Map each helper name to the factory parameters of its definitions."""
    helpers = {}
    for tokens in token_lists:
        for index in xrange(len(tokens) - 1):
            if tokens[index].value not in ("class", "def"):
                continue
            name = tokens[index + 1].value
            for list_index, spans in enumerate(_argument_lists(tokens, index + 2)):
                for arg_index, (start, end) in enumerate(spans):
                    values = [token.value for token in tokens[start:end]]
                    if any(tuple(values[i:i + 3]) == FACTORY_TYPE_END
                           for i in xrange(len(values) - 2)):
                        helpers.setdefault(name, set()).add(FactoryParam(
                            list_index, arg_index, _parameter_name(tokens, start, end)))
    return helpers


def _named_argument(tokens, start, end):
    if (start + 2 < end and tokens[start + 1].value == "=" and
            tokens[start + 2].value not in ("=", ">")):
        return tokens[start].value, start + 2
    return None, start


def _is_function_literal(tokens, start, end):
    while start < end and tokens[start].value == "{":
        start += 1
    if start >= end:
        return False
    first = tokens[start].value
    if first == "case":
        return True
    if first == "(":
        close = _matching_token(tokens, start)
        if close is not None and close + 1 < end and tokens[close + 1].value == "=>":
            return True
    # Placeholder syntax: `_` standing for an argument of the outermost call, as in
    # `new FooMeta(_, _, _, _)`; placeholders nested deeper belong to other functions.
    depth = 0
    for index in xrange(start, end - 1):
        value = tokens[index].value
        if value in OPEN_TO_CLOSE:
            depth += 1
        elif value in CLOSE_TO_OPEN:
            depth -= 1
        if depth == 1 and value in ("(", ",") and tokens[index + 1].value == "_":
            return True
    return False


def scan_tokens(path, tokens, helpers):
    violations = []
    for index, token in enumerate(tokens):
        params = helpers.get(token.value)
        if not params:
            continue
        names = set(param.name for param in params if param.name)
        positions = set((param.list_index, param.arg_index) for param in params)
        for list_index, spans in enumerate(_argument_lists(tokens, index + 1)):
            for arg_index, (start, end) in enumerate(spans):
                name, value_start = _named_argument(tokens, start, end)
                factory = name in names if name else (list_index, arg_index) in positions
                if factory and _is_function_literal(tokens, value_start, end):
                    violations.append(Violation(path, tokens[value_start].line, token.value))
    return violations


def scan_source(path, source, helpers=None):
    """Scan one source; without explicit helpers, use the helpers it defines."""
    tokens = tokenize(source)
    return tuple(scan_tokens(path, tokens, find_helpers([tokens]) if helpers is None
                             else helpers))


def _production_sources(root):
    for current_root, dirs, files in os.walk(root):
        dirs[:] = sorted(name for name in dirs
                         if name not in (".git", "target") and not name.startswith("."))
        relative_dir = "/" + os.path.relpath(current_root, root).replace(os.sep, "/") + "/"
        if "/src/main/" not in relative_dir:
            continue
        for filename in sorted(files):
            if filename.endswith(".scala"):
                yield os.path.join(current_root, filename)


def scan_tree(root):
    sources = []
    for absolute in _production_sources(root):
        relative = os.path.relpath(absolute, root).replace(os.sep, "/")
        try:
            with io.open(absolute, encoding="utf-8") as source_file:
                sources.append((relative, tokenize(source_file.read())))
        except UnicodeDecodeError as error:
            raise ValueError("{0}: {1}".format(relative, error))
    helpers = find_helpers([tokens for _, tokens in sources])
    violations = [violation for path, tokens in sources
                  for violation in scan_tokens(path, tokens, helpers)]
    return helpers, tuple(sorted(violations))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    args = parser.parse_args(argv)

    helpers, violations = scan_tree(os.path.abspath(args.root))
    if not helpers:
        sys.stderr.write("No replacement-rule helpers found under {0}; check --root.\n".format(
            args.root))
        return 2
    if violations:
        sys.stderr.write("Replacement-rule metadata factories must be named, "
                         "not function literals:\n")
        for violation in violations:
            sys.stderr.write("  {0}:{1}: argument to {2}\n".format(*violation))
        return 1

    print("Replacement-rule metadata factory lint passed ({0} helpers).".format(len(helpers)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
