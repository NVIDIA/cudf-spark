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
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{GpuFastSampleExec, GpuSampleExec}

import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{ExternalRDDScanExec, RDDScanExec, SampleExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.rapids.{GpuShuffleRegistry, RapidsShuffleInternalManagerBase}

/** Which GPU shuffles the UCX shuffle reclaim may free. */
private[execution] object UcxShuffleReclaimAdmission extends Logging {
  /**
   * Makes a GPU shuffle a candidate for the UCX shuffle reclaim when the driver plugin started
   * the reclaim, the shuffle's writers use the shuffle buffer catalog, and a rerun of its map side
   * reproduces its output. Call it once the dependency is built: registerShuffle and the
   * dependency's initializer both write checksumFallback, and admission must read the value that
   * the writers read.
   */
  def admit(exchange: SparkPlan, dependency: ShuffleDependency[_, _, _]): Unit = {
    if (GpuShuffleRegistry.get != null &&
        RapidsShuffleInternalManagerBase.usesShuffleBufferCatalog(dependency.shuffleHandle)) {
      if (mapSideIsReproducible(exchange)) {
        GpuShuffleRegistry.add(dependency.shuffleId)
      } else {
        logDebug(s"Shuffle ${dependency.shuffleId} waits for GC, since a rerun of its map side " +
          "may not reproduce its output")
      }
    }
  }

  /**
   * Whether a rerun of a plan's map side reproduces its output. A job that judged a reclaimed
   * shuffle available reruns part of its map stage after a fetch failure, and that failure can
   * drop an earlier shuffle's outputs on the same executor too. So the walk goes through earlier
   * shuffles and query stages, and stops only at a broadcast, whose value Spark builds once.
   *
   * It refuses a nondeterministic expression, a sample, which picks rows by their position in
   * its input, and a scan of an RDD, whose rerun the plan cannot see. It cannot see a value that
   * depends on the order rows arrive in under a deterministic expression, such as first() or a
   * floating-point sum.
   */
  def mapSideIsReproducible(plan: SparkPlan): Boolean = plan match {
    case _: BroadcastExchangeLike | _: BroadcastQueryStageExec => true
    case _: SampleExec | _: GpuSampleExec | _: GpuFastSampleExec => false
    case _: RDDScanExec | _: ExternalRDDScanExec[_] => false
    case stage: QueryStageExec => mapSideIsReproducible(stage.plan)
    case reused: ReusedExchangeExec => mapSideIsReproducible(reused.child)
    case node =>
      node.expressions.forall(_.deterministic) && node.children.forall(mapSideIsReproducible)
  }
}
