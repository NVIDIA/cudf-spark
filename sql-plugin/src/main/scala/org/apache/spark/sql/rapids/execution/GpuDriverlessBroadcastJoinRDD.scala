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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

private class GpuDriverlessBroadcastJoinPartition(
    override val index: Int,
    @transient private val streamRdd: RDD[_],
    @transient private val buildRdd: RDD[_]) extends Partition {
  var streamPartition: Partition = streamRdd.partitions(index)
  var buildPartition: Partition = buildRdd.partitions(0)

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    streamPartition = streamRdd.partitions(index)
    buildPartition = buildRdd.partitions(0)
    out.defaultWriteObject()
  }
}

/**
 * Makes the single-partition build an explicit dependency of every streamed partition.
 * Payload is fetched only when the executor needs to populate its build cache.
 */
private[execution] class GpuDriverlessBroadcastJoinRDD[A: ClassTag, B: ClassTag, C: ClassTag](
    private var streamRdd: RDD[A],
    private var buildRdd: RDD[B],
    join: (Iterator[A], () => Iterator[B]) => Iterator[C])
    extends RDD[C](streamRdd.context, Nil) {

  require(buildRdd.getNumPartitions == 1,
    s"driverless broadcast requires one build partition, found ${buildRdd.getNumPartitions}")

  override protected def getPartitions: Array[Partition] = streamRdd.partitions.map { part =>
    new GpuDriverlessBroadcastJoinPartition(part.index, streamRdd, buildRdd)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[GpuDriverlessBroadcastJoinPartition]
    streamRdd.preferredLocations(part.streamPartition)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[C] = {
    val part = split.asInstanceOf[GpuDriverlessBroadcastJoinPartition]
    // Operator timing wraps iterator consumption, not RDD.compute. Defer build/cache work
    // until consumption so it is timed and unused consumers do not initialize a build.
    new Iterator[C] {
      private lazy val delegate = {
        context.killTaskIfInterrupted()
        val stream = streamRdd.iterator(part.streamPartition, context)
        join(stream, () => buildRdd.iterator(part.buildPartition, context))
      }
      override def hasNext: Boolean = delegate.hasNext
      override def next(): C = delegate.next()
    }
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(streamRdd),
    new NarrowDependency[B](buildRdd) {
      override def getParents(partitionId: Int): Seq[Int] = Seq(0)
    })

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    streamRdd = null
    buildRdd = null
  }
}
