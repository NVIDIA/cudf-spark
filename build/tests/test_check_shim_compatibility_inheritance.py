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

from check_shim_compatibility import PreflightError, coverage, normalize_legacy_exact


class ShimCompatibilityInheritanceTest(unittest.TestCase):
    def test_preflight_normalizes_old_base_without_relaxing_selector_parser(self):
        old = b'/*** spark-rapids-shim-json-lines\n{"spark": "355"}\n' \
              b'spark-rapids-shim-json-lines ***/\n'
        self.assertIn(b'{"spark":["355"],"carryForward":"none"}',
                      normalize_legacy_exact(old))
        old_boolean = b'{"spark":["355"],"includeDescendantShims":true}'
        self.assertEqual(normalize_legacy_exact(old_boolean),
                         b'{"spark":["355"],"carryForward":"all"}')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        profiles = ''.join(
            '<profile><id>release%s</id><properties>%s</properties></profile>' %
            (target, '<shim.source.inherits>%s</shim.source.inherits>' % parent
             if parent else '')
            for target, parent in (('355', None), ('356', '355'), ('357', '356')))
        versions = ''.join('<spark%s.version>3.5.%s</spark%s.version>' %
                           (target, target[-1], target)
                           for target in ('355', '356', '357'))
        (self.root / 'pom.xml').write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<properties>%s</properties><profiles>%s</profiles></project>' %
            (versions, profiles), encoding='utf-8')
        shared = self.root / 'sql-plugin/src/main/scala/pkg/Caller.scala'
        shared.parent.mkdir(parents=True)
        shared.write_text('package pkg\nimport pkg.Example\nclass Caller\n', encoding='utf-8')

    def source(self, owner, selectors):
        path = self.root / 'sql-plugin/src/main' / ('spark' + owner) / 'scala/pkg/Example.scala'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('/*** spark-rapids-shim-json-lines\n%s\n'
                        'spark-rapids-shim-json-lines ***/\npackage pkg\nclass Example\n' %
                        '\n'.join(selectors), encoding='utf-8')
        return path

    def test_inherited_source_and_nearest_exact_overlay(self):
        self.source('355', ['{"spark":["355"],"carryForward":"all"}'])
        self.source('356', ['{"spark":["356"],"carryForward":"none"}'])
        result = coverage(self.root, ['sql-plugin'], None)
        self.assertEqual(result['status'], 'PASS')
        self.assertFalse(result['findings'])
        self.assertEqual(result['buildvers'], ['355', '356', '357'])

    def test_equal_distance_variants_are_a_blocking_duplicate(self):
        self.source('355', ['{"spark":["355","356"],"carryForward":"none"}'])
        self.source('356', ['{"spark":["356"],"carryForward":"none"}'])
        result = coverage(self.root, ['sql-plugin'], None)
        self.assertEqual(result['status'], 'BLOCKED')
        duplicate = next(finding for finding in result['findings']
                         if finding['kind'] == 'duplicate_source')
        self.assertEqual(duplicate['buildver'], '356')
        self.assertEqual(len(duplicate['providers']), 2)

    def test_unknown_introduction_rejected(self):
        self.source('355', ['{"spark":["999"],"carryForward":"all"}'])
        with self.assertRaisesRegex(PreflightError, 'Unknown Spark selector target 999'):
            coverage(self.root, ['sql-plugin'], None)


if __name__ == '__main__':
    unittest.main()
