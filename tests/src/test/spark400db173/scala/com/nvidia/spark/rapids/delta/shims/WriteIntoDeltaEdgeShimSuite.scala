/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOptions}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{LongType, StructType}

object WriteIntoDeltaEdgeShimSuite {
  val revision24 = "74719ffead181613a6858637ae9d6771bb243479"
  val revision25 = "2c6d9e667780c1deb163769ed814a3b4509e3d39"
  val defaultSchema: StructType = new StructType().add("default", LongType)
  val explicit: Seq[AnyRef] =
    Seq(null, SaveMode.Append, null, Seq.empty[String], Map.empty[String, String], null)

  // Only the erased JVM parameter types and the positions of the defaults matter here.
  case class PreviousWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitions: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalog: Option[Int] = Some(7),
      schemaInCatalog: Option[StructType] = Some(defaultSchema),
      clustering: Option[Int] = Some(9),
      alias: Option[Int] = None,
      snapshot: Option[Int] = None,
      jobGroup: Option[Int] = None,
      stats: Option[Int] = Some(13))

  case class CurrentWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitions: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalog: Option[Int] = Some(7),
      now: Option[Int] = Some(8),
      schemaInCatalog: Option[StructType] = Some(defaultSchema),
      clustering: Option[Int] = Some(10),
      alias: Option[Int] = None,
      snapshot: Option[Int] = None,
      jobGroup: Option[Int] = None,
      stats: Option[Int] = Some(14))

  object IncompatibleDefaults {
    def apply$default$7(): String = "not a Scala Option"
  }
}

class WriteIntoDeltaEdgeShimSuite extends AnyFunSuite {
  import WriteIntoDeltaEdgeShimSuite._

  test("DBR 17.3.24 resolves 13 parameters and uses the patch's own defaults") {
    val resolved = WriteIntoDeltaEdgeShim.resolve(revision24, classOf[PreviousWrite], PreviousWrite)
    val binding = resolved.fold(message => fail(message), identity)
    val defaults = binding.newWrite(explicit, None)
    assert(defaults.mode == SaveMode.Append)
    assert(defaults.schemaInCatalog.contains(defaultSchema))
    assert(defaults.catalog.contains(7) && defaults.clustering.contains(9))
    assert(defaults.stats.contains(13))

    val schema = new StructType().add("catalog", LongType)
    val overridden = binding.newWrite(explicit, Some(Some(schema)))
    assert(overridden == defaults.copy(schemaInCatalog = Some(schema)))
  }

  test("DBR 17.3.25 resolves 14 parameters and preserves the inserted default") {
    val resolved = WriteIntoDeltaEdgeShim.resolve(revision25, classOf[CurrentWrite], CurrentWrite)
    val binding = resolved.fold(message => fail(message), identity)
    val defaults = binding.newWrite(explicit, None)
    assert(defaults.schemaInCatalog.contains(defaultSchema))
    assert(defaults.catalog.contains(7) && defaults.now.contains(8))
    assert(defaults.clustering.contains(10) && defaults.stats.contains(14))

    val schema = new StructType().add("catalog", LongType)
    val overridden = binding.newWrite(explicit, Some(Some(schema)))
    assert(overridden == defaults.copy(schemaInCatalog = Some(schema)))
    assert(binding.newWrite(explicit, Some(None)) == defaults.copy(schemaInCatalog = None))
  }

  test("reject a known revision with the other patch's constructor") {
    val oldUnderNew = WriteIntoDeltaEdgeShim.resolve(
      revision25, classOf[PreviousWrite], PreviousWrite)
    val newUnderOld = WriteIntoDeltaEdgeShim.resolve(
      revision24, classOf[CurrentWrite], CurrentWrite)
    assert(oldUnderNew.left.toOption.exists(_.contains("NoSuchMethodException")))
    assert(newUnderOld.left.toOption.exists(_.contains("NoSuchMethodException")))
  }

  test("reject a missing default method") {
    val result = WriteIntoDeltaEdgeShim.resolve(revision25, classOf[CurrentWrite], PreviousWrite)
    assert(result.left.toOption.exists(_.contains("apply$default$14")))
  }

  test("reject a default method with the wrong JVM return type") {
    val result = WriteIntoDeltaEdgeShim.resolve(
      revision25, classOf[CurrentWrite], IncompatibleDefaults)
    assert(result.left.toOption.exists(_.contains("unexpected return type")))
  }

  test("reject unknown revisions before inspecting the command class") {
    val result = WriteIntoDeltaEdgeShim.resolve[CurrentWrite](
      "unsupported-build",
      fail("unknown revision must not inspect the command class"),
      fail("unknown revision must not inspect the companion"))
    assert(result == Left("unrecognized Databricks build unsupported-build"))
  }

  test("an unavailable JVM symbol becomes an unsupported binding") {
    val result = WriteIntoDeltaEdgeShim.resolve(
      revision25, classOf[CurrentWrite], throw new NoClassDefFoundError("test companion"))
    assert(result.left.toOption.exists(_.contains("NoClassDefFoundError")))
  }
}
