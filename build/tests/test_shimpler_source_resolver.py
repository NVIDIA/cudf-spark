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

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from shimpler_source_graph import load_source_graph
from shimpler_source_generator import generate_sources
from shimpler_source_resolver import (candidate_inherited_sources, discover_shim_modules,
                                     resolve_sources, scan_module_sources, validate_legacy_shadow)
from shimpler_source_targets import EXTERNAL_EXACT_TARGETS


def selector(descendants, *targets):
    return json.dumps({'spark': targets, 'carryForward': 'all' if descendants else 'none'},
                      separators=(',', ':'))


class SourceResolverTest(unittest.TestCase):
    def test_vendored_tools_match_pin(self):
        manifest = ROOT / 'build' / 'shimpler_source_tools.sha256'
        for entry in manifest.read_text().splitlines():
            if not entry or entry.startswith('#'):
                continue
            expected, filename = entry.split('  ', 1)
            actual = hashlib.sha256((ROOT / 'build' / filename).read_bytes()).hexdigest()
            self.assertEqual(expected, actual, filename)

    def test_generation_accepts_other_scala_profiles(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        known_targets = set().union(*graphs)
        with tempfile.TemporaryDirectory() as temp:
            for target in ('400', '500'):
                generated = Path(temp) / target
                selected = generate_sources(str(ROOT / 'sql-plugin'), str(generated),
                                            graphs[1], target, known_targets,
                                            EXTERNAL_EXACT_TARGETS)
                self.assertTrue(selected)

    def setUp(self):
        self.graph = {'355': None, '356': '355', '357': '356',
                      '420': None, '500': '420'}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.module = Path(self.temp.name) / 'nested' / 'module'

    def source(self, owner, name, selectors, language='scala', scope='main'):
        path = self.module / 'src' / scope / ('spark' + owner) / language / 'pkg' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if selectors is None:
            text = 'package pkg\nclass Example\n'
        else:
            text = ('/*** spark-rapids-shim-json-lines\n%s\n'
                    'spark-rapids-shim-json-lines ***/\npackage pkg\nclass Example\n'
                    % '\n'.join(selectors))
        path.write_text(text, encoding='utf-8')
        return str(path)

    def scan(self):
        return scan_module_sources(str(self.module), self.graph)

    def test_scan_scala_java_and_unannotated(self):
        scala = self.source('355', 'Example.scala', [selector(False, '355')])
        java = self.source('355', 'Other.java', [selector(False, '355')], 'java', 'test')
        unannotated = self.source('355', 'Skipped.scala', None)
        variants, skipped = self.scan()
        self.assertEqual(set(variant.physical_path for variant in variants), {scala, java})
        self.assertEqual(skipped, [unannotated])
        self.assertEqual({variant.logical_path for variant in variants},
                         {'src/main/scala/pkg/Example.scala', 'src/test/java/pkg/Other.java'})
        self.assertEqual(discover_shim_modules(self.temp.name), [str(self.module)])

    def test_scan_rejects_unsorted_or_duplicate_selectors(self):
        for selectors in ([selector(False, '356'), selector(False, '355')],
                          [selector(False, '355'), selector(True, '355')]):
            self.source('355', 'Example.scala', selectors)
            with self.assertRaisesRegex(ValueError, 'unique and sorted'):
                self.scan()

    def test_scan_rejects_multiple_selector_blocks(self):
        source = self.source('355', 'Example.scala', [selector(False, '355')])
        with open(source, 'a', encoding='utf-8') as output:
            output.write('/*** spark-rapids-shim-json-lines\n'
                         '%s\nspark-rapids-shim-json-lines ***/\n' % selector(False, '356'))
        with self.assertRaisesRegex(ValueError, 'Malformed shim selector block'):
            self.scan()

    def test_nearest_overlay_and_exact_only(self):
        first = self.source('355', 'Example.scala', [selector(True, '355')])
        overlay = self.source('357', 'Example.scala', [selector(True, '357')])
        only = self.source('500', 'Other.scala', [selector(False, '500')])
        variants, _ = self.scan()
        logical = 'src/main/scala/pkg/Example.scala'
        self.assertEqual(resolve_sources(self.graph, variants, '356')[logical].physical_path, first)
        selected = resolve_sources(self.graph, variants, '357')[logical]
        self.assertEqual(selected.physical_path, overlay)
        self.assertEqual(selected.ancestry_path, ('357',))
        self.assertEqual(resolve_sources(self.graph, variants, '500')
                         ['src/main/scala/pkg/Other.scala'].physical_path, only)
        self.assertNotIn('src/main/scala/pkg/Other.scala',
                         resolve_sources(self.graph, variants, '420'))

    def test_multiple_introduction_roots_and_exact_override(self):
        base = self.source('355', 'Example.scala', [selector(True, '355', '420')])
        override = self.source('500', 'Example.scala', [selector(False, '500')])
        variants, _ = self.scan()
        logical = 'src/main/scala/pkg/Example.scala'
        self.assertEqual(resolve_sources(self.graph, variants, '357')[logical].physical_path, base)
        self.assertEqual(resolve_sources(self.graph, variants, '420')[logical].physical_path, base)
        self.assertEqual(resolve_sources(self.graph, variants, '500')[logical].physical_path,
                         override)

    def test_ambiguous_variants_rejected(self):
        self.source('355', 'Example.scala', [selector(False, '356')])
        self.source('356', 'Example.scala', [selector(False, '356')])
        variants, _ = self.scan()
        with self.assertRaisesRegex(ValueError, 'Ambiguous shim sources for 356'):
            resolve_sources(self.graph, variants, '356')

    def test_nearer_override_wins_regardless_of_variant_order(self):
        self.source('355', 'Example.scala', [selector(True, '355')])
        self.source('356', 'Example.scala', [selector(True, '355')])
        override = self.source('357', 'Example.scala', [selector(False, '357')])
        variants, _ = self.scan()
        logical = 'src/main/scala/pkg/Example.scala'
        for ordered in (variants, list(reversed(variants))):
            self.assertEqual(resolve_sources(self.graph, ordered, '357')[logical].physical_path,
                             override)

    def test_shadow_keeps_exact_removals(self):
        exact = self.source('420', 'Example.scala', [selector(False, '420')])
        variants, _ = self.scan()
        self.assertEqual(validate_legacy_shadow(self.graph, variants), (exact,))
        candidates, _ = candidate_inherited_sources(self.graph, variants)
        self.assertEqual(resolve_sources(self.graph, candidates, '500'), {})

    def test_shadow_accepts_descendant_closed_membership(self):
        self.source('355', 'Example.scala', [selector(False, '355', '356', '357')])
        variants, _ = self.scan()
        self.assertEqual(validate_legacy_shadow(self.graph, variants), ())

    def test_versioned_package_path_stays_exact(self):
        source = self.source('357', 'spark357/Provider.scala', [selector(False, '357')])
        variants, _ = self.scan()
        self.assertEqual(validate_legacy_shadow(self.graph, variants), (source,))

    def test_shadow_requires_oracle_for_converted_sources(self):
        self.source('355', 'Example.scala', [selector(True, '355')])
        variants, _ = self.scan()
        with self.assertRaisesRegex(ValueError, 'pre-conversion oracle is required'):
            validate_legacy_shadow(self.graph, variants)

    def test_new_patch_child_keeps_every_non_identity_source(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        targets = set(graphs[0]) | set(graphs[1])
        modules = []
        for module in discover_shim_modules(str(ROOT)):
            variants, _ = scan_module_sources(module, targets, EXTERNAL_EXACT_TARGETS)
            modules.append((Path(module).relative_to(ROOT).as_posix(), variants))

        for graph in graphs:
            patch_targets = set(target for target in graph
                                if target.isdigit() and target != '500')
            same_line_parents = set(parent for target, parent in graph.items()
                                    if target in patch_targets and parent in patch_targets and
                                    target[:2] == parent[:2])
            leaves = sorted(patch_targets - same_line_parents)
            for leaf in leaves:
                child = leaf + 'next'
                child_graph = dict(graph)
                child_graph[child] = leaf
                for module_path, variants in modules:
                    parent_sources = resolve_sources(graph, variants, leaf)
                    child_sources = resolve_sources(child_graph, variants, child)
                    for logical, parent_source in parent_sources.items():
                        if any(part.startswith('spark') and part[5:8].isdigit()
                               for part in logical.split('/')):
                            continue
                        self.assertIn(logical, child_sources,
                                      '%s at child of %s' % (module_path, leaf))
                        self.assertEqual(child_sources[logical].physical_path,
                                         parent_source.physical_path,
                                         '%s at child of %s' % (module_path, leaf))

    def test_repository_rapids_error_utils_owners(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        targets = set(graphs[0]) | set(graphs[1])
        variants, _ = scan_module_sources(str(ROOT / 'sql-plugin'), targets,
                                          EXTERNAL_EXACT_TARGETS)
        logical = 'src/main/scala/org/apache/spark/sql/rapids/shims/RapidsErrorUtils.scala'
        expected = {
            '350db143': 'spark350db143',
            '400db173': 'spark350db143',
            '410db183': 'spark350db143',
            '400': 'spark400',
            '401': 'spark400',
        }
        for graph in graphs:
            for target, owner in expected.items():
                if target in graph:
                    selected = resolve_sources(graph, variants, target)[logical]
                    self.assertIn('/src/main/%s/' % owner, selected.physical_path)

    def test_source_inspector_reports_actual_selection_and_infers_scala_tree(self):
        command = [str(ROOT / 'build' / 'shimpler'), 'sources', '--target', '359',
                   '--module', 'sql-plugin', '--format=json']
        output = subprocess.check_output(command, universal_newlines=True)
        result = json.loads(output)
        self.assertEqual(result['scala'], '2.12')
        self.assertTrue(any(row['selector_kind'] == 'descendants' for row in result['sources']))
        self.assertTrue(any(row['selector_kind'] == 'exact' for row in result['sources']))
        self.assertTrue(any(row['physical_path'].endswith('PartitionedFileUtilsShim.scala')
                            and row['introduction_target'] == '358'
                            for row in result['sources']))
        command[3] = '500'
        result = json.loads(subprocess.check_output(command, universal_newlines=True))
        self.assertEqual(result['scala'], '2.13')
        self.assertTrue(result['sources'])


if __name__ == '__main__':
    unittest.main()
