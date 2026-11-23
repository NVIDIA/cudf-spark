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

import imp
import io
import os
import shutil
import tempfile
import unittest


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "check_replacement_rule_meta_lambdas.py")
LINT = imp.load_source("check_replacement_rule_meta_lambdas", SCRIPT)

# Registry helpers shaped like GpuOverrides.expr, the ExprRule constructor and ReplacementRule.wrap.
HELPERS = u"""
object GpuOverrides {
  def expr[INPUT <: Expression](
      desc: String,
      pluginChecks: ExprChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => BaseExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = ???
}
class ExprRule[INPUT <: Expression](
    protected var doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => BaseExprMeta[INPUT],
    desc: String)
abstract class ReplacementRule {
  final def wrap(func: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]],
      DataFromReplacementRule) => WRAP_TYPE): this.type = this
}
"""
SCALA_HELPERS = LINT.find_helpers([LINT.tokenize(HELPERS)])


class ReplacementRuleMetaFactoryLintSuite(unittest.TestCase):
    def assert_violations(self, source, expected):
        violations = LINT.scan_source("Rules.scala", source, SCALA_HELPERS)
        self.assertEqual(expected, [(item.line, item.helper) for item in violations])

    def test_discovers_factory_parameters(self):
        self.assertEqual(
            {"expr": set([LINT.FactoryParam(0, 2, "doWrap")]),
             "ExprRule": set([LINT.FactoryParam(0, 0, "doWrap")]),
             "wrap": set([LINT.FactoryParam(0, 0, "func")])},
            SCALA_HELPERS)

    def test_rejects_lambdas_in_core_shim_and_provider_registries(self):
        # Parameter names and enclosing objects do not matter.
        self.assert_violations(u"""
          object GpuStringOverrides {
            expr[Conv]("conv", checks, (conv, conf, p, dataFromReplacementRule) =>
              new GpuConvMeta(conv, conf, p, dataFromReplacementRule))
          }
          trait Spark400PlusCommonShims {
            GpuOverrides.expr[VariantGet]("variant", checks,
              (a, b, c, d) => new GpuVariantGetMeta(a, b, c, d))
          }
          class DeltaProviderBase {
            new ExprRule[IncrementMetric]((i, c, p, r) => GpuIncrementMetricMeta(i, c, p, r), "d")
            rule.wrap((e, c, p, r) => new WrappedMeta(e, c, p, r))
          }
        """, [(3, "expr"), (8, "expr"), (11, "ExprRule"), (12, "wrap")])

    def test_rejects_any_function_literal_body(self):
        self.assert_violations(u"""
          expr[A]("block", checks, (a, conf, p, r) => {
            new BlockMeta(a, conf, p, r)
          })
          expr[A]("line break", checks, (a, conf, p, r) =>
            new LineBreakMeta(a, conf, p, r))
          expr[A]("anonymous base", checks, (a, conf, p, r) =>
            new CastExprMetaBase[A](a, conf, p, r) { override val toType = StringType })
          expr[A]("brace lambda", checks, { (a, conf, p, r) =>
            log(a)
            new FooMeta(a, conf, p, r)
          })
          expr[A]("partial function", checks, { case (a, c, p, r) => new FooMeta(a, c, p, r) })
          expr[A]("conditional", checks, (a, conf, p, r) =>
            if (enabled) createFooMeta(a, conf, p, r) else createBarMeta(a, conf, p, r))
        """, [(2, "expr"), (5, "expr"), (7, "expr"), (9, "expr"), (13, "expr"), (14, "expr")])

    def test_rejects_placeholder_factories_and_named_arguments(self):
        self.assert_violations(u"""
          expr[A]("placeholder", checks, new FooMeta(_, _, _, _))
          expr[A]("typed", checks, GpuFooMeta(_: A, _: RapidsConf, _, _))
          expr[A]("named", checks, doWrap = (a, conf, p, r) => new FooMeta(a, conf, p, r))
          expr[A](doWrap = createFooMeta, pluginChecks = checks, desc = "reordered")
        """, [(2, "expr"), (3, "expr"), (4, "expr")])

    def test_accepts_named_factories(self):
        self.assert_violations(u"""
          expr[A]("method", checks, createFooMeta)
          expr[A]("companion", checks, GpuFooMeta)
          expr[A]("curried", checks, createElementAtMeta(nullOnInvalidAccess))
          expr[A]("eta", checks, createFooMeta _)
          new ExprRule[A](createFooMeta, "constructor")
          rule.wrap(createWrappedMeta)
        """, [])

    def test_only_checks_factory_arguments_of_helper_calls(self):
        self.assert_violations(u"""
          expr[A]("other arguments", ExprChecks(types.map(t => t.nested())), createFooMeta)
          expr[A]("non-factory literal", (a, b, c, d) => checks, createFooMeta)
          expr[A]("nested placeholder", checks, createFooMeta(types.map(_.nested())))
          rows.scan(0)(_ + _)
          ByteBuffer.wrap(bytes)
          metrics.map(m => (m, conf, p, r) => new FooMeta(m, conf, p, r))
          def expr(desc: String): Unit = ()
        """, [])

    def test_ignores_comments_and_literals(self):
        self.assert_violations(u"""
          // expr[A]("comment", checks, (a, c, p, r) => new FooMeta(a, c, p, r))
          /* expr[A]("block", checks, (a, c, p, r) => new FooMeta(a, c, p, r)) */
          val doc = \"\"\"say "hi\"\"\"\"; expr[A](desc, c, (a, b, c, d) => x)
          expr[A](s"${"("}", c, (a, b, c, d) => new FooMeta(a, b, c, d))
          expr[A]('d, checks, (a, c, p, r) => new FooMeta(a, c, p, r))
          expr[A]("after literals", checks, (a, c, p, r) \u21d2 new FooMeta(a, c, p, r))
        """, [(4, "expr"), (5, "expr"), (6, "expr"), (7, "expr")])

    def test_main_scans_production_sources_with_cross_file_helpers(self):
        root = tempfile.mkdtemp()
        try:
            def write(relative, text):
                path = os.path.join(root, *relative.split("/"))
                if not os.path.isdir(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path))
                with io.open(path, "w", encoding="utf-8") as source_file:
                    source_file.write(text)

            self.assertEqual(2, LINT.main(["--root", root]))
            write("sql-plugin/src/main/scala/GpuOverrides.scala", HELPERS)
            write("tests/src/test/scala/Suite.scala",
                  u'expr[A]("test", c, (a, b, c, d) => new TestMeta(a, b, c, d))')
            write("delta-lake/src/main/scala/Provider.scala", u'expr[A]("ok", c, createMeta)')
            self.assertEqual(0, LINT.main(["--root", root]))
            write("delta-lake/src/main/scala/Provider.scala",
                  u'expr[A]("bad", c, (a, b, c, d) => new BadMeta(a, b, c, d))')
            self.assertEqual(1, LINT.main(["--root", root]))
        finally:
            shutil.rmtree(root)


if __name__ == "__main__":
    unittest.main()
