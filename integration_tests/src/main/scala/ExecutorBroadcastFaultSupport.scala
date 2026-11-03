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
package org.apache.spark.sql.rapids.testing

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.types.LongType

/** Test-only injection, compiled separately from the product JAR with Spark's Scala compiler. */
object ExecutorBroadcastFaultSupport {
  def collectAfterRemovingBuildOutputs(
      session: SparkSession,
      frame: Dataset[Row],
      slots: Int): Array[String] = session.withActive {
    SQLExecution.withNewExecutionId(frame.queryExecution, Some("lost-broadcast-build-output")) {
      collectWithMissingBuild(session, frame, slots)
    }
  }

  private def collectWithMissingBuild(
      session: SparkSession,
      frame: Dataset[Row],
      slots: Int): Array[String] = {
    require(frame.schema.fields.map(_.dataType).toSeq == Seq(LongType, LongType),
      "This fixture checks complete rows with exactly two long columns")
    val executed = frame.queryExecution.executedPlan
    val finalPlan = executed match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.finalPhysicalPlan
      case other => other
    }
    val joins = finalPlan.collect {
      case node if node.getClass.getSimpleName == "GpuDriverlessBroadcastHashJoinExec" => node
    }
    require(joins.size == 1, s"Expected one executor broadcast join, found ${joins.size}")
    val join = joins.head
    // Build the actual output RDD once. Creating it completes and admits the build map stage;
    // collecting this same RDD after deletion must fetch and recover the missing build output.
    val result = finalPlan.execute()
    val build = join.getClass.getMethod("buildPlan").invoke(join).asInstanceOf[SparkPlan]
    val shuffleId = build.getClass.getMethod("shuffleId").invoke(build).asInstanceOf[Int]
    val observations = session.sparkContext.parallelize(0 until slots, slots).barrier()
      .mapPartitions { _ =>
        // Barrier scheduling reserves all slots, and the driver verifies that both executors
        // were visited. No barrier synchronization request is needed to remove these outputs.
        // Resolve the catalog through the active shim loader, as the product does. This helper
        // depends only on Spark APIs and does not add a product dependency on testing code.
        val shim = Class.forName("com.nvidia.spark.rapids.ShimLoader$")
        val shimModule = shim.getField("MODULE$").get(null)
        val loader = shim.getMethod("getShimClassLoader").invoke(shimModule)
          .asInstanceOf[ClassLoader]
        val env = Class.forName("org.apache.spark.sql.rapids.GpuShuffleEnv$", true, loader)
        val module = env.getField("MODULE$").get(null)
        val catalog = env.getMethod("getCatalog").invoke(module)
        val active = catalog.getClass.getMethod("hasActiveShuffle", classOf[Int])
        val hadOutput = active.invoke(catalog, Int.box(shuffleId)).asInstanceOf[Boolean]
        // Delete only catalog output. Keep Spark's map registrations and broadcast cache intact
        // so the subsequent read must discover the missing output and trigger map recovery.
        catalog.getClass.getMethod("unregisterShuffle", classOf[Int])
          .invoke(catalog, Int.box(shuffleId))
        require(!active.invoke(catalog, Int.box(shuffleId)).asInstanceOf[Boolean])
        Iterator((SparkEnv.get.executorId, hadOutput))
      }.collect()
    require(observations.map(_._1).distinct.length == 2,
      s"Injection did not visit two executors: ${observations.toSeq}")
    require(observations.exists(_._2), "The injected shuffle had no registered output")
    val rows = result.map(row => s"${row.getLong(0)}:${row.getLong(1)}").collect()
    Array(s"shuffle=$shuffleId,observations=${observations.toSeq}") ++ rows
  }
}
