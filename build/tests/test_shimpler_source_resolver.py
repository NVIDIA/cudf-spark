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
import xml.etree.ElementTree as ET
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

    def test_representative_repository_provider_maps(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        variants, _ = scan_module_sources(str(ROOT / 'sql-plugin'),
                                          set().union(*graphs), EXTERNAL_EXACT_TARGETS)
        selections = {}
        for target in ('359', '413', '350db143', '400db173', '410db183', '500'):
            graph = next(graph for graph in graphs if target in graph)
            selections[target] = resolve_sources(graph, variants, target)

        baseline = ROOT / 'build' / 'tests' / 'shimpler_source_provider_baseline.tsv'
        expected_paths = {target: set() for target in selections}
        for line in baseline.read_text().splitlines():
            if line.startswith('#') or not line:
                continue
            targets, logical, provider = line.split('\t')
            for target in targets.split(','):
                expected_paths[target].add(logical)
                with self.subTest(target=target, logical=logical):
                    self.assertIn(logical, selections[target])
                    actual = Path(selections[target][logical].physical_path)
                    self.assertEqual(actual.relative_to(ROOT).as_posix(), provider)
        baselined_paths = set().union(*expected_paths.values())
        for target, expected in expected_paths.items():
            self.assertEqual(set(selections[target]) & baselined_paths, expected, target)

    def test_other_changed_selectors_keep_their_targets(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        targets = set().union(*graphs)
        cases = (
            ('iceberg/iceberg-1-11-x',
             'src/main/scala/com/nvidia/spark/rapids/iceberg/iceberg111x/'
             'IcebergProviderImpl.scala', 'spark402',
             ('402', '403', '404'), ('400', '401', '402', '403', '404', '411')),
            ('tests', 'src/test/scala/com/nvidia/spark/rapids/WindowGroupLimitExecSuite.scala',
             'spark350', ('400', '401', '402', '403', '404'),
             ('400', '401', '402', '403', '404', '411')),
            ('tests', 'src/test/scala/com/nvidia/spark/rapids/shims/'
             'Spark40GpuDataSourceCustomMetricsSuite.scala', 'spark350db143',
             ('350db143', '400', '400db173', '401', '402', '403', '404',
              '410db183', '411'),
             ('350db143', '400', '400db173', '401', '402', '403', '404',
              '410db183', '411', '412', '413', '420', '500')),
        )
        for module, logical, owner, expected, checked in cases:
            variants, _ = scan_module_sources(str(ROOT / module), targets,
                                              EXTERNAL_EXACT_TARGETS)
            for target in checked:
                graph = next(graph for graph in graphs if target in graph)
                selected = resolve_sources(graph, variants, target).get(logical)
                with self.subTest(module=module, logical=logical, target=target):
                    if target in expected:
                        self.assertIsNotNone(selected)
                        self.assertIn('/src/%s/%s/' % (
                            'test' if module == 'tests' else 'main', owner),
                            selected.physical_path)
                    elif selected is not None:
                        self.assertNotIn('/src/%s/%s/' % (
                            'test' if module == 'tests' else 'main', owner),
                            selected.physical_path)

    def test_iceberg_suites_stop_at_stub_profiles(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        variants, _ = scan_module_sources(str(ROOT / 'tests'),
                                          set().union(*graphs), EXTERNAL_EXACT_TARGETS)
        suite_names = {
            'GpuIcebergVariantPostProcessorSuite.scala',
            'GpuIcebergVariantSchemaSuite.scala',
            'GpuMultiThreadIcebergParquetReaderSuite.scala',
            'GpuPostProcessorSuite.scala',
            'IcebergDeletionVectorSuite.scala',
            'IcebergS3InputFileSuite.scala',
        }
        expected = {
            '411': suite_names,
            '412': suite_names,
            '413': suite_names,
            '414': {'IcebergDeletionVectorSuite.scala'},
            '420': set(),
            '500': set(),
        }
        for target, expected_suites in expected.items():
            graph = next(graph for graph in graphs if target in graph)
            selected = resolve_sources(graph, variants, target)
            found = {Path(logical).name for logical in selected
                     if Path(logical).name in suite_names}
            self.assertEqual(found, expected_suites, target)

    def test_interval_and_writer_suites_cover_later_spark4(self):
        graphs = (load_source_graph(str(ROOT / 'pom.xml')),
                  load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml')))
        variants, _ = scan_module_sources(str(ROOT / 'tests'),
                                          set().union(*graphs), EXTERNAL_EXACT_TARGETS)
        suite_names = {
            'IntervalCastSuite.scala', 'IntervalDivisionSuite.scala',
            'IntervalMultiplySuite.scala', 'GpuIntervalUtilsTest.scala',
            'ConcurrentWriterMetricsSuite.scala', 'OrcEncryptionSuite.scala',
            'RapidsShuffleThreadedWriterSuite.scala',
        }
        for target in ('400', '411', '412', '413', '420', '500'):
            graph = next(graph for graph in graphs if target in graph)
            selected = resolve_sources(graph, variants, target)
            found = {Path(logical).name for logical in selected if
                     Path(logical).name in suite_names}
            self.assertEqual(found, suite_names, target)

    def test_failed_generation_cannot_leave_a_valid_cache_marker(self):
        def local_tag(node):
            return node.tag.rsplit('}', 1)[-1]

        for pom in (ROOT / 'pom.xml', ROOT / 'scala2.13' / 'pom.xml'):
            root = ET.parse(str(pom)).getroot()
            execution = next(node for node in root.iter() if local_tag(node) == 'execution'
                             and any(local_tag(child) == 'id' and
                                     child.text == 'generate-inherited-shim-sources'
                                     for child in node))
            target = next((node for node in execution.iter() if local_tag(node) == 'target'),
                          None)
            self.assertIsNotNone(target, pom)
            branches = [node for node in target.iter() if local_tag(node) == 'then']
            generation_branch = next(branch for branch in branches
                                     if any(local_tag(child) == 'generateShimSources'
                                            for child in branch))
            actions = list(generation_branch)
            deletion = next(node for node in actions if local_tag(node) == 'delete'
                            and any(local_tag(child) == 'fileset' and child.attrib == {
                                'dir': '${spark.shim.dest}', 'includes': '.shim-sources-*'}
                                    for child in node))
            generation = next(node for node in actions
                              if local_tag(node) == 'generateShimSources')
            creation = next(node for node in actions if local_tag(node) == 'touch'
                            and node.get('file') == '${rapids.shim.sources.cache.marker}')
            self.assertLess(actions.index(deletion), actions.index(generation), pom)
            self.assertLess(actions.index(generation), actions.index(creation), pom)

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
