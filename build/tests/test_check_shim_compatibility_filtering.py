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

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

import check_shim_compatibility as checker
from test_shim_compatibility_impact import DEFAULT_GRAPH, POM_PATHS, pom, source


REPO = 'NVIDIA/cudf-spark'
SOURCE = 'sql-plugin/src/main/spark355/scala/pkg/Example.scala'


class ShimCompatibilityFilteringTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git('init', '-q')
        self.base = self.commit({path: pom() for path in POM_PATHS})
        self.reference = checker.git_texts(self.root, self.base, set(POM_PATHS))

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True,
                                       stderr=subprocess.PIPE).strip()

    def commit(self, files):
        for name, value in files.items():
            path = self.root / name
            if value is None:
                path.unlink(missing_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(value)
        self.git('add', '--all')
        self.git('-c', 'user.name=Shim filtering test', '-c',
                 'user.email=shim-filter-test@example.invalid', 'commit', '-q',
                 '--signoff', '-m', 'Temporary filtering fixture')
        return self.git('rev-parse', 'HEAD')

    def branch(self, files):
        self.git('checkout', '-q', '--detach', self.base)
        return self.commit(files)

    def patch(self, name, **kwargs):
        patcher = mock.patch.object(checker, name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def profile_branch(self, target):
        changed = pom(profile_changes={target: {'dependency': 'updated'}})
        return self.branch({path: changed for path in POM_PATHS})

    def impact(self, head):
        return checker.revision_impact(self.root, self.base, head, self.reference)

    def candidate(self, number, head):
        return dict(number=number, title='Peer %d' % number,
                    url='https://github.com/NVIDIA/cudf-spark/pull/%d' % number,
                    state='OPEN', headRefOid=head, isDraft=False)

    def filtering(self, current, peer, paths):
        with mock.patch.object(checker, 'fetch_peer', side_effect=lambda root, repo, head: head):
            return checker.candidate_filter(self.root, REPO, self.base, current,
                                            {'headRefOid': peer}, paths, self.reference)

    def filtered_report(self):
        current = self.profile_branch('355')
        peer = self.profile_branch('400')
        evidence = self.filtering(self.impact(current), peer, set(POM_PATHS))
        item = dict(number=1, title='Peer 1', url='https://github.com/NVIDIA/cudf-spark/pull/1',
                    state='OPEN', head=peer, draft=False, reasons=['shared matrix path'],
                    files=list(POM_PATHS), joint_candidate=True, filter=evidence)
        report = dict(status='NO_RELATED_PRS', base=self.base, head=current, base_branch='main',
                      related=[], filtered_prs=[item], candidates_scanned=1)
        return current, peer, report

    def run_filtered_advisory(self, discovery, graphql, verify=None):
        with mock.patch.object(checker, 'related_prs', return_value=copy.deepcopy(discovery)), \
                mock.patch.object(checker, 'graphql', side_effect=graphql), \
                mock.patch.object(checker, 'verify_review_refs', side_effect=verify) as verified:
            result = checker.advisory(self.root, REPO, self.base, discovery['head'],
                                      None, 'main', 99)
        return result, verified

    def test_git_blobs_preserve_missing_sides_spaces_and_reject_symlinks(self):
        original = self.branch({'old source.scala': 'original\n'})
        changed = self.commit({'old source.scala': None, 'dir/new source.scala': 'updated\n'})
        paths = {'old source.scala', 'dir/new source.scala', 'missing.scala'}
        before = checker.git_texts(self.root, original, paths)
        after = checker.git_texts(self.root, changed, paths)
        self.assertEqual(before, {'old source.scala': 'original\n',
                                  'dir/new source.scala': None, 'missing.scala': None})
        self.assertEqual(after, {'old source.scala': None,
                                 'dir/new source.scala': 'updated\n', 'missing.scala': None})
        os.symlink('pom.xml', self.root / 'linked-pom.xml')
        linked = self.commit({})
        with self.assertRaises(checker.PreflightError):
            checker.git_texts(self.root, linked, {'linked-pom.xml'})

    def test_real_common_ancestor_proofs_filter_disjoint_but_keep_inherited_overlap(self):
        current = self.profile_branch('355')
        current_impact = self.impact(current)
        advanced_base = self.profile_branch('400')
        advanced_reference = checker.git_texts(self.root, advanced_base, set(POM_PATHS))
        common_impact = checker.revision_impact(self.root, advanced_base, current, advanced_reference)
        self.assertEqual(common_impact['comparison_base'], self.base)
        self.assertEqual(common_impact['profiles'], ['355', '356', '357'])
        for target, status in (('400', 'FILTERED'), ('356', 'RETAINED')):
            with self.subTest(target=target):
                peer = self.profile_branch(target)
                evidence = self.filtering(current_impact, peer, set(POM_PATHS))
                self.assertEqual(evidence['status'], status, evidence)
                self.assertEqual(evidence['base'], self.base)
                self.assertEqual(evidence['head'], peer)
                self.assertEqual(evidence['comparison_base'], self.base)
                self.assertEqual(evidence['current_profiles'], ['355', '356', '357'])
                self.assertIn(target, evidence['peer_profiles'])

    def test_profile_exclusion_edits_retain_cross_profile_matrix_interactions(self):
        namespace = '{http://maven.apache.org/POM/4.0.0}'
        tree = ET.fromstring(pom())
        profiles = tree.find(namespace + 'profiles')
        tree.remove(profiles)
        tree.insert(4, profiles)
        properties = profiles[0].find(namespace + 'properties')
        exclusion = ET.SubElement(properties, namespace + 'dyn.shim.excluded.releases')
        exclusion.text = '400'
        excluded = ET.tostring(tree, encoding='unicode')
        properties.remove(exclusion)
        enabled = ET.tostring(tree, encoding='unicode')
        foo = SOURCE.replace('Example.scala', 'Foo.scala')
        caller = 'sql-plugin/src/main/spark400/scala/pkg/Caller.scala'
        baseline_files = {path: excluded for path in POM_PATHS}
        baseline_files.update({foo: source(body='class Foo'),
                               caller: source(('400',), body='class Caller')})
        self.base = self.branch(baseline_files)
        self.reference = checker.git_texts(self.root, self.base, set(POM_PATHS))
        current = self.branch({path: enabled for path in POM_PATHS})
        peer = self.branch({caller: source(('400',), body='import pkg.Foo\nclass Caller')})
        joint = self.git('merge-tree', '--write-tree', current, peer).splitlines()[0]
        results = []
        for revision in (self.base, current, peer, joint):
            with tempfile.TemporaryDirectory() as temporary:
                snapshot = Path(temporary)
                for name, value in checker.git_texts(
                        self.root, revision, set(POM_PATHS) | {foo, caller}).items():
                    if value is not None:
                        destination = snapshot / name
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        destination.write_text(value)
                results.append(checker.coverage(snapshot, ['sql-plugin'], None))
        self.assertEqual([result['status'] for result in results],
                         ['PASS', 'PASS', 'PASS', 'BLOCKED'])
        self.assertEqual([(finding['kind'], finding['buildver'], finding['symbol'])
                          for finding in results[-1]['findings']],
                         [('missing_implementation', '400', 'pkg.Foo')])
        impact = self.impact(current)
        self.assertEqual(impact['status'], 'UNKNOWN', impact)
        self.assertEqual(self.filtering(impact, peer, {caller})['status'], 'RETAINED_UNCERTAIN')

    def test_new_profile_current_is_retained_without_filter_fetch(self):
        graph = dict(DEFAULT_GRAPH, **{'358': '357'})
        current = self.branch({path: pom(graph=graph) for path in POM_PATHS})
        impact = self.impact(current)
        self.assertEqual(impact['status'], 'UNKNOWN')
        peer = self.branch({SOURCE: source()})
        with mock.patch.object(checker, 'fetch_peer') as fetched:
            evidence = checker.candidate_filter(self.root, REPO, self.base, impact,
                                                {'headRefOid': peer}, {SOURCE}, self.reference)
        self.assertEqual(evidence['status'], 'RETAINED_UNCERTAIN')
        fetched.assert_not_called()

    def test_peer_metadata_change_is_retained_when_current_impact_is_unknown(self):
        peer = self.branch({path: pom(description='updated') for path in POM_PATHS})
        evidence = self.filtering(dict(status='UNKNOWN', profiles=[], reason='global change'),
                                  peer, set(POM_PATHS))
        self.assertEqual(evidence['status'], 'RETAINED_UNCERTAIN', evidence)

    def test_metadata_change_and_new_build_reference_are_retained_in_both_directions(self):
        metadata = self.branch({path: pom(description='updated') for path in POM_PATHS})
        for text in (pom(global_dependency='${project.description}'),
                     pom(profile_changes={'400': {'dependency': '${project.description}'}})):
            referencing = self.branch({path: text for path in POM_PATHS})
            for current, peer in ((metadata, referencing), (referencing, metadata)):
                with self.subTest(current=current, peer=peer):
                    evidence = self.filtering(self.impact(current), peer, set(POM_PATHS))
                    self.assertIn(evidence['status'], ('RETAINED', 'RETAINED_UNCERTAIN'), evidence)

    def test_indirect_metadata_alias_and_new_reference_are_retained_in_both_directions(self):
        baseline = pom(description='${project.name}').replace(
            '<description>', '<name>old</name><description>', 1)
        self.base = self.branch({path: baseline for path in POM_PATHS})
        self.reference = checker.git_texts(self.root, self.base, set(POM_PATHS))
        metadata = self.branch({path: baseline.replace('<name>old</name>', '<name>new</name>')
                                for path in POM_PATHS})
        referencing = pom(description='${project.name}', profile_changes={
            '400': {'dependency': '${project.description}'}}).replace(
                '<description>', '<name>old</name><description>', 1)
        peer = self.branch({path: referencing for path in POM_PATHS})
        for current, other in ((metadata, peer), (peer, metadata)):
            with self.subTest(current=current, peer=other):
                evidence = self.filtering(self.impact(current), other, set(POM_PATHS))
                self.assertIn(evidence['status'], ('RETAINED', 'RETAINED_UNCERTAIN'), evidence)

    def test_metadata_change_and_proven_unreferencing_profile_remain_independent(self):
        metadata = self.branch({path: pom(description='updated') for path in POM_PATHS})
        profile = self.profile_branch('400')
        for current, peer in ((metadata, profile), (profile, metadata)):
            with self.subTest(current=current, peer=peer):
                evidence = self.filtering(self.impact(current), peer, set(POM_PATHS))
                self.assertEqual(evidence['status'], 'FILTERED', evidence)

    def test_shared_sources_and_global_tools_are_retained_without_filter_fetch(self):
        current = self.impact(self.profile_branch('355'))
        for path in ('sql-plugin/src/main/scala/pkg/Caller.scala',
                     'build/shimpler_source_graph.py', 'build/shim_compatibility_impact.py'):
            with self.subTest(path=path), mock.patch.object(checker, 'fetch_peer') as fetched:
                evidence = checker.candidate_filter(self.root, REPO, self.base, current,
                                                    {'headRefOid': self.base}, {path}, self.reference)
                self.assertEqual(evidence['status'], 'RETAINED_UNCERTAIN')
                fetched.assert_not_called()

    def test_malformed_inputs_and_filter_fetch_failure_keep_candidates(self):
        current = self.impact(self.profile_branch('355'))
        peer = self.branch({'pom.xml': '<project><profiles>'})
        evidence = self.filtering(current, peer, {'pom.xml'})
        self.assertEqual(evidence['status'], 'RETAINED_UNCERTAIN')
        with mock.patch.object(checker, 'fetch_peer', side_effect=checker.PreflightError('unavailable')):
            evidence = checker.candidate_filter(self.root, REPO, self.base, current,
                                                {'headRefOid': peer}, {'pom.xml'}, self.reference)
        self.assertEqual(evidence['status'], 'RETAINED_UNCERTAIN')
        self.assertIn('retain', evidence['reason'].lower())

    def test_proven_current_formatting_only_needs_no_peer_fetch(self):
        current = self.branch({path: pom().replace('><', '>\n<') for path in POM_PATHS})
        impact = self.impact(current)
        self.assertEqual((impact['status'], impact['profiles']), ('KNOWN', []))
        with mock.patch.object(checker, 'fetch_peer') as fetched:
            evidence = checker.candidate_filter(self.root, REPO, self.base, impact,
                                                {'headRefOid': self.base},
                                                {'sql-plugin/src/main/scala/pkg/Caller.scala'},
                                                self.reference)
        self.assertEqual(evidence['status'], 'FILTERED')
        fetched.assert_not_called()

    def discovery_fixture(self):
        current = self.profile_branch('355')
        peers = [self.candidate(1, self.profile_branch('400')),
                 self.candidate(2, self.profile_branch('356'))]
        self.git('checkout', '-q', '--detach', current)
        return current, peers

    def discovery_patches(self, peers, second=None):
        self.patch('remote_base', return_value=self.base)
        opened = self.patch('open_prs', side_effect=[peers, peers if second is None else second])
        self.patch('merged_prs', return_value=[])
        self.patch('candidate_files', side_effect=lambda root, owner, name, peer:
                   (peer['headRefOid'], set(POM_PATHS)))
        self.patch('fetch_peer', side_effect=lambda root, repo, head: head)
        return opened

    def test_discovery_separates_filtered_and_retained_peers_using_immutable_proofs(self):
        current, peers = self.discovery_fixture()
        opened = self.discovery_patches(peers)
        report = checker.related_prs(self.root, REPO, self.base, current, None, 'main', None,
                                    include_worktree=False)
        self.assertEqual(report['status'], 'NEEDS_REVIEW')
        self.assertEqual([peer['number'] for peer in report['related']], [2])
        self.assertEqual([peer['number'] for peer in report['filtered_prs']], [1])
        evidence = report['filtered_prs'][0]['filter']
        self.assertEqual((evidence['base'], evidence['head'], evidence['comparison_base']),
                         (self.base, peers[0]['headRefOid'], self.base))
        self.assertEqual(opened.call_count, 2)
        self.assertFalse(report['dirty'])

    def test_discovery_revalidates_filtered_peer_heads_too(self):
        current, peers = self.discovery_fixture()
        moved = copy.deepcopy(peers)
        moved[0]['headRefOid'] = 'c' * 40
        self.discovery_patches(peers, moved)
        with self.assertRaisesRegex(checker.PreflightError, 'heads changed'):
            checker.related_prs(self.root, REPO, self.base, current, None, 'main', None,
                                include_worktree=False)

    def test_all_filtered_advisory_revalidates_peers_and_own_refs_and_reports_exclusions(self):
        current, peer, discovery = self.filtered_report()
        live = {'p1': dict(state='OPEN', headRefOid=peer, baseRefName='main')}
        result, verified = self.run_filtered_advisory(discovery, lambda *args, **kwargs: live)
        self.assertEqual(result['status'], 'NO_RELATED_PRS')
        verified.assert_called_once()
        self.assertEqual(result['related'], [])
        output = self.root / 'output'
        checker.write_advisory(result, output)
        data = json.loads((output / 'report.json').read_text())
        summary = data['joint_check_summary']
        self.assertEqual((summary['filtered'], summary['completed'], summary['not_checked']), (1, 0, 0))
        markdown = (output / 'report.md').read_text()
        self.assertIn('Filtered PRs', markdown)
        self.assertIn(result['filtered_prs'][0]['filter']['reason'], markdown)
        self.assertIn(peer, markdown)
        self.assertIn(self.base, markdown)

    def test_changed_filtered_peer_is_restored_as_unchecked_uncertainty(self):
        _, _, discovery = self.filtered_report()
        live = {'p1': dict(state='OPEN', headRefOid='c' * 40, baseRefName='main')}
        result, verified = self.run_filtered_advisory(discovery, lambda *args, **kwargs: live)
        self.assertEqual(result['status'], 'INCOMPLETE')
        verified.assert_not_called()
        self.assertFalse(result.get('filtered_prs'))
        self.assertEqual(result['related'][0]['filter']['status'], 'RETAINED_UNCERTAIN')
        self.assertEqual(result['related'][0]['joint_check']['status'], 'NOT_CHECKED')
        summary = checker.joint_check_summary(result)
        self.assertEqual((summary['filtered'], summary['completed'], summary['not_checked']), (0, 0, 1))

    def test_filtered_proofs_are_restored_after_final_api_or_target_failure(self):
        for failure in ('api', 'target'):
            with self.subTest(failure=failure):
                _, peer, discovery = self.filtered_report()
                live = {'p1': dict(state='OPEN', headRefOid=peer, baseRefName='main')}
                def graphql(*args, **kwargs):
                    if failure == 'api':
                        raise checker.PreflightError('final API unavailable')
                    return live
                verification = checker.PreflightError('target moved') if failure == 'target' else None
                result, _ = self.run_filtered_advisory(discovery, graphql, verification)
                self.assertEqual(result['status'], 'INCOMPLETE')
                self.assertFalse(result.get('filtered_prs'))
                self.assertEqual(result['related'][0]['joint_check']['status'], 'NOT_CHECKED')
                self.assertEqual(result['related'][0]['filter']['status'], 'RETAINED_UNCERTAIN')


if __name__ == '__main__':
    unittest.main()
