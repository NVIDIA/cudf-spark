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

from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from shimpler_source_generator import generate_sources


class SourceGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.module = Path(self.temp.name) / 'module'
        self.generated = self.module / 'target' / 'spark356' / 'generated' / 'src'
        self.graph = {'355': None, '356': '355', '357': '356'}

    def source(self, owner, name, selectors, body='package pkg\nclass Example\n', scope='main'):
        path = self.module / 'src' / scope / ('spark' + owner) / 'scala' / 'pkg' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('/*** spark-rapids-shim-json-lines\n%s\n'
                        'spark-rapids-shim-json-lines ***/\n%s' %
                        ('\n'.join(selectors), body), encoding='utf-8')
        return path

    def test_inherited_source_and_exact_overlay(self):
        base = self.source('355', 'Example.scala',
                           ['{"spark":["355"],"carryForward":"all"}'])
        overlay = self.source('356', 'Example.scala',
                              ['{"spark":["356"],"carryForward":"none"}'])
        output = self.generated / 'main' / 'scala' / 'pkg' / 'Example.scala'
        selected = generate_sources(str(self.module), str(self.generated), self.graph, '356')
        self.assertEqual(selected['src/main/scala/pkg/Example.scala'].physical_path,
                         str(overlay))
        self.assertTrue(output.is_symlink())
        self.assertEqual(output.resolve(), overlay)
        other_root = self.module / 'target' / 'spark357' / 'generated' / 'src'
        generate_sources(str(self.module), str(other_root), self.graph, '357')
        self.assertEqual((other_root / 'main' / 'scala' / 'pkg' / 'Example.scala').resolve(), base)

    def test_exact_target_outside_active_profile_graph(self):
        self.source('355', 'Example.scala',
                    ['{"spark":["355"],"carryForward":"all"}'])
        vendor = self.source('330db', 'Example.scala',
                             ['{"spark":["330db"],"carryForward":"none"}'])
        graph = dict(self.graph)
        graph['330db'] = None
        selected = generate_sources(str(self.module), str(self.generated), graph, '330db',
                                    set(self.graph), {'330db'})
        self.assertEqual(selected['src/main/scala/pkg/Example.scala'].physical_path, str(vendor))

    def test_removes_stale_outputs_and_interpolates_package(self):
        self.source('355', 'Interpolated.scala',
                    ['{"spark":["356"],"carryForward":"none"}'],
                    'package pkg.$_spark.version.classifier_\nclass Example\n')
        stale = self.generated / 'main' / 'scala' / 'pkg' / 'Stale.scala'
        stale.parent.mkdir(parents=True)
        stale.write_text('stale', encoding='utf-8')
        marker = self.generated / '.shim-source-cache-marker'
        marker.write_text('', encoding='utf-8')
        generate_sources(str(self.module), str(self.generated), self.graph, '356')
        self.assertFalse(stale.exists())
        self.assertTrue(marker.exists())
        output = self.generated / 'main' / 'scala' / 'pkg' / 'Interpolated.scala'
        self.assertFalse(output.is_symlink())
        self.assertIn('package pkg.spark356', output.read_text(encoding='utf-8'))

    def test_error_does_not_remove_previous_outputs(self):
        self.source('355', 'Example.scala',
                    ['{"spark":["356"],"carryForward":"none"}'])
        self.source('356', 'Example.scala',
                    ['{"spark":["356"],"carryForward":"none"}'])
        previous = self.generated / 'main' / 'scala' / 'pkg' / 'Previous.scala'
        previous.parent.mkdir(parents=True)
        previous.write_text('previous', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'Ambiguous shim sources'):
            generate_sources(str(self.module), str(self.generated), self.graph, '356')
        self.assertTrue(previous.exists())

    def test_unannotated_source_does_not_remove_previous_outputs(self):
        unannotated = self.module / 'src' / 'main' / 'spark355' / 'scala' / 'pkg' / 'Example.scala'
        unannotated.parent.mkdir(parents=True)
        unannotated.write_text('package pkg\nclass Example\n', encoding='utf-8')
        previous = self.generated / 'main' / 'scala' / 'pkg' / 'Previous.scala'
        previous.parent.mkdir(parents=True)
        previous.write_text('previous', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'No shim selector block'):
            generate_sources(str(self.module), str(self.generated), self.graph, '356')
        self.assertTrue(previous.exists())


if __name__ == '__main__':
    unittest.main()
