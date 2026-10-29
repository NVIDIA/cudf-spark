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

import importlib.util
import os
import tempfile
import unittest


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                      "check_replacement_rule_meta_lambdas.py")
SPEC = importlib.util.spec_from_file_location("check_replacement_rule_meta_lambdas", SCRIPT)
LINT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LINT)


class ReplacementRuleMetaLambdaLintSuite(unittest.TestCase):
    def assert_factories(self, source, expected):
        violations = LINT.scan_source("src/main/scala/Test.scala", source)
        self.assertEqual(expected, [item.factories for item in violations])

    def test_parameter_names_and_enclosing_object_do_not_bypass_check(self):
        self.assert_factories("""
          object AnyProvider {
            rule((value, settings, owner, replacement) =>
              new ExampleMeta(value, settings, owner, replacement))
          }
        """, [("ExampleMeta",)])

    def test_checks_shim_and_external_provider_shapes(self):
        for source in (
                "shim((exec, conf, parent, rule) => new ShimExecMeta(exec, conf, parent, rule))",
                "provider((scan, conf, parent, rule) => new ProviderScanMeta(scan, conf, parent, rule))"):
            self.assertEqual(1, len(LINT.scan_source("src/main/scala/Test.scala", source)))

    def test_checks_block_and_line_break_before_constructor(self):
        self.assert_factories("""
          rule((a, b, c, d) => {
            new BlockMeta(
              a, b, c, d)
          })
        """, [("BlockMeta",)])

    def test_checks_conditional_factory_once(self):
        self.assert_factories("""
          rule((a, b, c, d) => if (enabled) {
            new EnabledMeta(a, b, c, d)
          } else {
            new DisabledMeta(a, b, c, d)
          })
        """, [("EnabledMeta", "DisabledMeta")])

    def test_nested_lambda_constructor_is_only_attributed_to_nested_lambda(self):
        self.assert_factories("""
          outer((a, b, c, d) =>
            inner((e, f, g, h) => new InnerMeta(e, f, g, h)))
        """, [("InnerMeta",)])

    def test_checks_qualified_generic_and_anonymous_constructor(self):
        self.assert_factories("""
          rule((a: A[X, Y], b, c, d) =>
            new pkg.GenericMeta[X, Y](a, b, c, d) { override def value = a })
        """, [("pkg.GenericMeta",)])

    def test_ignores_named_factory_comments_and_literals(self):
        self.assert_factories("""
          rule((a, b, c, d) => NamedRuleMeta)
          // (a, b, c, d) => new CommentMeta(a, b, c, d)
          val example = "(a, b, c, d) => new StringMeta(a, b, c, d)"
        """, [])

    def test_requires_four_parameters(self):
        self.assert_factories(
            "rule((a, b, c) => new ThreeArgumentMeta(a, b, c))", [])

    def test_semicolon_ends_unbraced_lambda_body(self):
        self.assert_factories("""
          val factory = (a, b, c, d) => named(a, b, c, d);
          val unrelated = new UnrelatedMeta(a, b, c, d)
        """, [])

    def test_inferred_semicolon_ends_unbraced_lambda_body(self):
        self.assert_factories("""
          val factory = (a, b, c, d) => named(a, b, c, d)
          val unrelated = new UnrelatedMeta(a, b, c, d)
        """, [])

    def test_main_requires_zero_violations(self):
        with tempfile.TemporaryDirectory() as root:
            source_dir = os.path.join(root, "module", "src", "main", "scala")
            os.makedirs(source_dir)
            source = os.path.join(source_dir, "Rules.scala")
            with open(source, "w", encoding="utf-8") as source_file:
                source_file.write("rule((a, b, c, d) => new ExampleMeta(a, b, c, d))")
            self.assertEqual(1, LINT.main(["--root", root]))
            with open(source, "w", encoding="utf-8") as source_file:
                source_file.write("rule(ExampleMeta)")
            self.assertEqual(0, LINT.main(["--root", root]))


if __name__ == "__main__":
    unittest.main()
