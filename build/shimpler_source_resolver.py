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

"""Resolve shim source variants for generation and source-selection inspection.

The shadow check derives introduction points only where a legacy membership
set is descendant-closed; other sets retain exact membership so removals
cannot be silently inherited. This module supports the build task's Jython 2.7 runtime.
"""

from collections import namedtuple
import os
import re

from shimpler_source_graph import source_lineage, validate_source_graph
from shimpler_source_selectors import (SourceSelector, derive_introductions,
                                      inherited_membership, parse_selector_line, spark_minor)


SHIM_COMMENT = re.compile(r'/\*\*\* spark-rapids-shim-json-lines\s*\n(.*?)\n'
                          r'spark-rapids-shim-json-lines \*\*\*/', re.DOTALL)
SourceVariant = namedtuple('SourceVariant', 'logical_path physical_path selectors')
SourceSelection = namedtuple('SourceSelection',
                             'logical_path physical_path introduced_at selector_kind ancestry_path')
VERSIONED_PATH = re.compile(r'^spark[0-9]{3}(?:[a-z][a-z0-9]*)?$')
SOURCE_LANGUAGE_PATH = re.compile(r'^(?:java|scala(?:-[0-9]+\.[0-9]+)?)$')


def _has_versioned_path(logical_path):
    return any(VERSIONED_PATH.match(part) for part in logical_path.split('/'))


def scan_module_sources(module_root, valid_targets, allowed_external_targets=()):
    """Return annotated Scala/Java shim variants and unannotated files in shim roots."""
    variants = []
    unannotated = []
    for scope in ('main', 'test'):
        scope_root = os.path.join(module_root, 'src', scope)
        if not os.path.isdir(scope_root):
            continue
        for owner in sorted(os.listdir(scope_root)):
            owner_root = os.path.join(scope_root, owner)
            if not VERSIONED_PATH.match(owner) or not os.path.isdir(owner_root):
                continue
            languages = []
            for child in sorted(os.listdir(owner_root)):
                child_root = os.path.join(owner_root, child)
                if not os.path.isdir(child_root):
                    continue
                if not SOURCE_LANGUAGE_PATH.match(child):
                    raise ValueError('Unexpected source directory %s' % child_root)
                languages.append(child)
            for language in languages:
                language_root = os.path.join(owner_root, language)
                if not os.path.isdir(language_root):
                    continue
                for directory, subdirs, files in os.walk(language_root):
                    subdirs.sort()
                    for filename in sorted(files):
                        if not filename.endswith(('.java', '.scala')):
                            continue
                        physical = os.path.join(directory, filename)
                        with open(physical, 'r') as source_file:
                            contents = source_file.read()
                        match = SHIM_COMMENT.search(contents)
                        if match is None:
                            unannotated.append(physical)
                            continue
                        if contents.count('spark-rapids-shim-json-lines') != 2:
                            raise ValueError('Malformed shim selector block in %s' % physical)
                        lines = [line.strip() for line in match.group(1).splitlines() if line.strip()]
                        if not lines:
                            raise ValueError('Empty shim selector block in %s' % physical)
                        relative = os.path.relpath(physical, language_root)
                        logical = os.path.join('src', scope, language, relative).replace(os.sep, '/')
                        try:
                            selectors = tuple(parse_selector_line(line, valid_targets,
                                                                 allowed_external_targets)
                                              for line in lines)
                        except ValueError as error:
                            raise ValueError('%s: %s' % (physical, error))
                        if _has_versioned_path(logical) and any(
                                selector.kind != 'exact' for selector in selectors):
                            raise ValueError('%s: versioned source paths require '
                                             'carryForward none' % physical)
                        anchors = [min(selector.targets) for selector in selectors]
                        if anchors != sorted(set(anchors)):
                            raise ValueError('%s: shim selectors must be unique and sorted' % physical)
                        variants.append(SourceVariant(logical, physical, selectors))
    return variants, unannotated


def discover_shim_modules(repository_root):
    """Find module roots with a versioned source directory, including nested modules."""
    modules = []
    for directory, subdirs, _ in os.walk(repository_root):
        subdirs[:] = sorted(child for child in subdirs
                            if child not in ('.git', 'target', 'worktrees', 'node_modules'))
        src_root = os.path.join(directory, 'src')
        if 'src' in subdirs:
            for scope in ('main', 'test'):
                scope_root = os.path.join(src_root, scope)
                if not os.path.isdir(scope_root):
                    continue
                if any(VERSIONED_PATH.match(child) and
                       os.path.isdir(os.path.join(scope_root, child))
                       for child in os.listdir(scope_root)):
                    modules.append(directory)
                    subdirs.remove('src')
                    break
    return modules


def _matching_selection(variant, lineage):
    matches = []
    for selector in variant.selectors:
        if selector.kind == 'exact':
            if lineage[0] in selector.targets:
                matches.append((0, lineage[0], selector.kind))
        else:
            for distance, ancestor in enumerate(lineage):
                if (ancestor in selector.targets and
                        (selector.kind != 'minor' or
                         spark_minor(ancestor) == spark_minor(lineage[0]))):
                    matches.append((distance, ancestor, selector.kind))
                    break
    return min(matches) if matches else None


def resolve_sources_with_ties(graph, variants, target):
    """Select nearest variants and return equal-distance source conflicts."""
    lineage = source_lineage(graph, target)
    selected = {}
    ties = {}
    for variant in variants:
        match = _matching_selection(variant, lineage)
        if match is None:
            continue
        distance, introduction, kind = match
        previous = selected.get(variant.logical_path)
        if previous is None or distance < previous[0]:
            selection = SourceSelection(variant.logical_path, variant.physical_path,
                                        introduction, kind, tuple(lineage[:distance + 1]))
            selected[variant.logical_path] = (distance, selection)
            ties.pop(variant.logical_path, None)
        elif distance == previous[0]:
            ties.setdefault(variant.logical_path,
                            [previous[1].physical_path]).append(variant.physical_path)
    return dict((logical, pair[1]) for logical, pair in selected.items()), ties


def resolve_sources(graph, variants, target):
    """Select the nearest applicable physical variant of each logical source."""
    selected, ties = resolve_sources_with_ties(graph, variants, target)
    if ties:
        logical = sorted(ties)[0]
        first, second = sorted(ties[logical])[:2]
        raise ValueError('Ambiguous shim sources for %s at %s: %s and %s' %
                         (target, logical, first, second))
    return selected


def candidate_inherited_sources(graph, variants):
    """Derive safe introduction selectors from current exact annotations.

    Non-closed sets and source paths embedding their exact target remain exact.
    The returned variants are only a shadow model, not a request to rewrite
    annotations or alter source generation.
    """
    validate_source_graph(graph)
    candidates = []
    non_inheritable = []
    for variant in variants:
        if any(selector.kind != 'exact' for selector in variant.selectors):
            raise ValueError('A captured pre-conversion oracle is required for %s' %
                             variant.logical_path)
        all_exact = set(target for selector in variant.selectors if selector.kind == 'exact'
                        for target in selector.targets)
        exact = all_exact.intersection(graph)
        external = all_exact.difference(graph)
        if external:
            non_inheritable.append(variant.physical_path)
            candidates.append(variant._replace(
                selectors=(SourceSelector('exact', tuple(sorted(all_exact))),)))
            continue
        if not exact:
            continue
        selectors = []
        introductions = derive_introductions(graph, exact)
        target_specific = _has_versioned_path(variant.logical_path)
        if not target_specific and inherited_membership(graph, introductions) == exact:
            selectors.append(SourceSelector('descendants', introductions))
        else:
            non_inheritable.append(variant.physical_path)
            selectors.append(SourceSelector('exact', tuple(sorted(exact))))
        candidates.append(variant._replace(selectors=tuple(selectors)))
    return candidates, tuple(sorted(non_inheritable))


def validate_legacy_shadow(graph, variants, targets=None):
    """Compare exact membership with candidate inheritance for selected targets.

    Returns physical sources that must retain exact membership for now.
    """
    validate_source_graph(graph)
    targets = tuple(sorted(graph)) if targets is None else tuple(targets)
    for target in targets:
        if target not in graph:
            raise ValueError('Unknown shadow target %s' % target)
    candidates, non_inheritable = candidate_inherited_sources(graph, variants)
    for target in targets:
        legacy = resolve_sources(graph, variants, target)
        proposed = resolve_sources(graph, candidates, target)
        for logical in sorted(set(legacy) | set(proposed)):
            legacy_source = legacy.get(logical)
            proposed_source = proposed.get(logical)
            legacy_path = None if legacy_source is None else legacy_source.physical_path
            proposed_path = None if proposed_source is None else proposed_source.physical_path
            if legacy_path != proposed_path:
                raise ValueError('Shim shadow mismatch for %s at %s: legacy %s, inherited %s' %
                                 (target, logical, legacy_path, proposed_path))
    return non_inheritable
