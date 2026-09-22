/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.util.parsing.combinator.RegexParsers

import com.nvidia.spark.rapids.PathInstruction

import org.apache.spark.SparkConf

object GetJsonObjectShim {
  // Copied from Apache Spark 3.5.3 JsonPathParser in jsonExpressions.scala.
  private object JsonPathParser extends RegexParsers {
    import com.nvidia.spark.rapids.PathInstruction._

    def root: Parser[Char] = '$'

    def long: Parser[Long] = "\\d+".r ^? {
      case x => x.toLong
    }

    // parse `[*]` and `[123]` subscripts
    def subscript: Parser[List[PathInstruction]] =
      for {
        operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
      } yield {
        Subscript :: operand :: Nil
      }

    // parse `.name` or `['name']` child expressions
    def named: Parser[List[PathInstruction]] =
      for {
        name <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']"
      } yield {
        Key :: Named(name) :: Nil
      }

    // child wildcards: `..`, `.*` or `['*']`
    def wildcard: Parser[List[PathInstruction]] =
      (".*" | "['*']") ^^^ List(Wildcard)

    def node: Parser[List[PathInstruction]] =
      wildcard |
        named |
        subscript

    val expression: Parser[List[PathInstruction]] = {
      phrase(root ~> rep(node) ^^ (x => x.flatten))
    }

    def parse(str: String): Option[List[PathInstruction]] = {
      this.parseAll(expression, str) match {
        case Success(result, _) =>
          Some(result)

        case _ =>
          None
      }
    }
  }

  private[rapids] def parse(
      str: String,
      _conf: SparkConf): Option[List[PathInstruction]] = JsonPathParser.parse(str)

  /**
   * Spark 3.x uses the legacy quoted-name parser. The Dataproc runtimes that backport
   * SPARK-46761 use the Spark 3.5.3-specific shim instead.
   */
  def parse(str: String): Option[List[PathInstruction]] = JsonPathParser.parse(str)
}
