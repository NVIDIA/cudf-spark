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
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from check_shim_compatibility import overlap_reasons, path_signals, review_scope


SHIM = 'sql-plugin/src/main/spark355/scala/pkg/Example.scala'
CALLER = 'sql-plugin/src/main/scala/pkg/Caller.scala'


def source_selection_tools(pom):
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    return [include.attrib['name'] for include in ET.parse(pom).findall(
        ".//m:checksum[@totalproperty='rapids.shim.sources.tools.checksum']"
        '/m:fileset/m:include', namespace)]


class ShimCompatibilityScopeTest(unittest.TestCase):
    def test_maven_source_selection_inputs_trigger_review(self):
        # Derive the contract from Maven so adding a generation dependency also
        # requires scope detection, even when that PR changes no POM or shim.
        for relative in ('pom.xml', 'scala2.13/pom.xml'):
            tools = source_selection_tools(ROOT / relative)
            self.assertTrue(tools, relative)
            for path in tools:
                with self.subTest(pom=relative, path=path):
                    self.assertTrue(review_scope({path}))
                    self.assertTrue(path_signals({path})[1])

    def test_source_selection_tools_relate_to_shims_in_both_directions(self):
        for path in source_selection_tools(ROOT / 'pom.xml'):
            with self.subTest(path=path):
                self.assertTrue(overlap_reasons({path}, {SHIM}))
                self.assertTrue(overlap_reasons({SHIM}, {path}))

    def test_source_selection_tools_relate_to_profile_changes(self):
        for path in source_selection_tools(ROOT / 'pom.xml'):
            for pom in ('pom.xml', 'scala2.13/pom.xml'):
                with self.subTest(path=path, pom=pom):
                    self.assertTrue(overlap_reasons({path}, {pom}))
                    self.assertTrue(overlap_reasons({pom}, {path}))

    def test_source_selection_tools_flag_shared_shim_callers(self):
        for path in source_selection_tools(ROOT / 'pom.xml'):
            with self.subTest(path=path):
                self.assertTrue(overlap_reasons({path}, {CALLER}))
                self.assertTrue(overlap_reasons({CALLER}, {path},
                                                references={'scala/pkg/Example.scala'}))

    def test_unrelated_tooling_docs_and_tests_stay_out_of_scope(self):
        paths = (
            'docs/dev/shims.md',
            'build/SHIMPLER_SOURCE_TOOLS.md',
            'build/tests/test_shimpler_source_resolver.py',
            'build/tests/shimpler_source_provider_baseline.tsv',
            'build/__pycache__/shimpler_source_resolver.cpython-312.pyc',
            'build/shimpler_source_notes.md',
            'build/unrelated.py',
            'build/shimpler',
            'build/shimpler_source_tools.sha256',
        )
        for path in paths:
            with self.subTest(path=path):
                self.assertFalse(review_scope({path}))
                self.assertFalse(any(path_signals({path})))
                self.assertFalse(overlap_reasons({path}, {SHIM}))
                self.assertFalse(overlap_reasons({SHIM}, {path}))

    def test_existing_shim_scope_is_preserved(self):
        paths = (
            SHIM, CALLER, 'pom.xml', 'scala2.13/pom.xml',
            'build/shim_source_task.py', 'build/get_buildvers.py',
            'build/dyn_shim_detection.py', 'build/buildall',
            'shim-deps/spark355/pom.xml',
            'build/check_shim_compatibility.py', 'build/shim_compatibility_impact.py',
            '.github/workflows/shim-compatibility.yml',
            '.agents/skills/check-shim-compatibility/SKILL.md',
        )
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(review_scope({path}))


if __name__ == '__main__':
    unittest.main()
