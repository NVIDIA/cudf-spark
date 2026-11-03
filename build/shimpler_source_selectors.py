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

"""Parse Spark source selectors and check proposed inheritance against exact membership.

This module is compatible with Shimplify's Jython 2.7 runtime. It does not
change source generation; callers must compare inherited and legacy selections
before making inherited selectors authoritative.
"""

from collections import namedtuple
import json

from shimpler_source_graph import validate_source_graph


try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)


SourceSelector = namedtuple('SourceSelector', 'kind targets other_components')


def parse_selector_line(line, graph, allow_unmapped_exact=False):
    """Parse one shim JSON line, retaining any non-Spark component selectors.

    Public POMs do not define private/vendor targets already present in exact
    annotations. A repository-wide public query may preserve those legacy
    strings, but new inheritance selectors must name a known graph target.
    """
    entry = json.loads(line)
    if not isinstance(entry, dict) or 'spark' not in entry:
        raise ValueError('Shim selector must contain a spark field')

    spark = entry['spark']
    if isinstance(spark, string_types):
        kind = 'exact'
        targets = (spark,)
    elif isinstance(spark, dict):
        if len(spark) != 1 or next(iter(spark)) not in ('introduced', 'only'):
            raise ValueError('Spark selector must have exactly one of introduced or only')
        kind = next(iter(spark))
        value = spark[kind]
        if isinstance(value, string_types):
            targets = (value,)
        elif kind == 'introduced' and isinstance(value, list):
            targets = tuple(value)
        else:
            raise ValueError('%s selector must name a target%s' %
                             (kind, ' or nonempty target list' if kind == 'introduced' else ''))
    else:
        raise ValueError('Spark selector must be a target or selector object')

    if not targets or any(not isinstance(target, string_types) or not target for target in targets):
        raise ValueError('Spark selector targets must be nonempty strings')
    if len(set(targets)) != len(targets):
        raise ValueError('Duplicate Spark selector target')
    for target in targets:
        if target not in graph and not (kind == 'exact' and allow_unmapped_exact):
            raise ValueError('Unknown Spark selector target %s' % target)

    other_components = dict((key, value) for key, value in entry.items() if key != 'spark')
    return SourceSelector(kind, targets, other_components)


def derive_introductions(graph, exact_targets):
    """Return candidate introduction nodes for a legacy exact-membership set."""
    validate_source_graph(graph)
    exact_targets = set(exact_targets)
    for target in exact_targets:
        if target not in graph:
            raise ValueError('Unknown exact-membership target %s' % target)
    return tuple(sorted(target for target in exact_targets if graph[target] not in exact_targets))


def inherited_membership(graph, introductions):
    """Return targets reached by one or more candidate introduction nodes."""
    validate_source_graph(graph)
    introductions = set(introductions)
    for target in introductions:
        if target not in graph:
            raise ValueError('Unknown introduction target %s' % target)
    membership = set()
    for target in graph:
        current = target
        while current is not None:
            if current in introductions:
                membership.add(target)
                break
            current = graph[current]
    return membership


def inheritance_mismatches(graph, exact_targets):
    """Return targets where candidate inheritance differs from exact membership."""
    exact_targets = set(exact_targets)
    introductions = derive_introductions(graph, exact_targets)
    return tuple(sorted(exact_targets.symmetric_difference(inherited_membership(graph, introductions))))
