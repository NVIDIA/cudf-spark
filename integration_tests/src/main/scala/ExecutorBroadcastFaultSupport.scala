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

import org.apache.spark.{BarrierTaskContext, SparkEnv}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan

/** Test-only injection, compiled separately from the product JAR with Spark's Scala compiler. */
object ExecutorBroadcastFaultSupport {
  def removeBuildOutputs(session: SparkSession, frame: Dataset[Row], slots: Int): String = {
    val joins = frame.queryExecution.executedPlan.collect {
      case node if node.getClass.getSimpleName == "GpuShuffleBroadcastHashJoinExec" => node
    }
    require(joins.size == 1, s"Expected one executor broadcast join, found ${joins.size}")
    val join = joins.head
    // This completes and admits the map stage without opening any executor build reader.
    join.executeColumnar()
    val build = join.getClass.getMethod("buildPlan").invoke(join).asInstanceOf[SparkPlan]
    val shuffleId = build.getClass.getMethod("shuffleId").invoke(build).asInstanceOf[Int]
    val observations = session.sparkContext.parallelize(0 until slots, slots).barrier()
      .mapPartitions { _ =>
        BarrierTaskContext.get().barrier()
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
    s"shuffle=$shuffleId,observations=${observations.toSeq}"
  }
}
