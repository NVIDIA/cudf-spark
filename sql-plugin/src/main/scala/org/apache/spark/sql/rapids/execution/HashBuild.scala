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

import java.util.concurrent.{CompletableFuture, ExecutionException}

import scala.collection.mutable
import scala.util.control.NonFatal

import ai.rapids.cudf.{DistinctHashJoin => CudfDistinctHashJoin,
  HashJoin => CudfHashJoin, Table}
import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuBuildSide, GpuColumnVector,
  GpuExpression, GpuMetric, GpuProjectExec, GpuSemaphore, NoopMetric, NvtxRegistry,
  SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.spill.SharedRecomputableHandle

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi,
  RightOuter}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Metrics associated with the reusable hash-build lifecycle. */
case class HashBuildMetrics(
    builds: GpuMetric = NoopMetric,
    rebuilds: GpuMetric = NoopMetric,
    reuses: GpuMetric = NoopMetric)

/**
 * Identifies derived hash-build state within a [[HashBuildCache]].
 * The same build-side data can be cached and reused by multiple joins. The key includes the
 * projection, canonicalized join keys, and null-handling semantics to disambiguate.
 */
case class HashBuildKey(
    sourceProjection: Seq[Seq[Expression]],
    projectedKeys: Seq[Expression],
    compareNullsEqual: Boolean,
    filterOutNulls: Boolean)

object HashBuildKey {
  def fromExpressions(
      sourceProjection: Seq[Seq[Expression]],
      boundKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean): HashBuildKey = {
    HashBuildKey(
      sourceProjection,
      boundKeys.map(_.canonicalized),
      compareNullsEqual,
      filterOutNulls)
  }
}

/**
 * The cuDF hash primitive selected for one stream batch. `requiredBuildSide` specifies
 * if the join has a physical side constraint e.g. left or right outer/semi/anti.
 * `exactCountJoinType` specifies when the primitive can provide an exact output count
 * before execution, and which JoinType will provide it.
 */
sealed trait HashJoinPrimitive {
  def requiredBuildSide: Option[GpuBuildSide]
  def exactCountJoinType: Option[JoinType]
}

private[execution] object HashJoinPrimitive {
  case object Inner extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = None
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.Inner)
  }

  case object LeftOuter extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.LeftOuter)
  }

  case object RightOuter extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildLeft)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.RightOuter)
  }

  case object LeftSemi extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  case object LeftAnti extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  final case class Distinct(joinType: JoinType, planBuildSide: GpuBuildSide)
      extends HashJoinPrimitive {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(planBuildSide)
    override val exactCountJoinType: Option[JoinType] = None
  }

  /** Map a directly executed Catalyst join type to its cuDF hash primitive. */
  def direct(joinType: JoinType): HashJoinPrimitive = joinType match {
    case _: InnerLike => Inner
    case org.apache.spark.sql.catalyst.plans.LeftOuter => LeftOuter
    case org.apache.spark.sql.catalyst.plans.RightOuter => RightOuter
    case org.apache.spark.sql.catalyst.plans.LeftSemi => LeftSemi
    case org.apache.spark.sql.catalyst.plans.LeftAnti => LeftAnti
    case other => throw new IllegalStateException(s"unsupported hash join primitive $other")
  }
}

/**
 * A hash operation resolved once for a stream batch. This owns the selected [[HashProbeBackend]].
 */
private[execution] final class ResolvedHashJoin(
    private val backend: HashProbeBackend,
    primitive: HashJoinPrimitive,
    val exactOutputRows: Option[Long]) extends AutoCloseable {
  def isCached: Boolean = backend.isCached

  def execute(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    backend.execute(primitive, leftKeys, rightKeys, exactOutputRows)
  }

  // Apply a post-join condition using the AST orientation for the resolved physical build side.
  def filterInner(
      innerMaps: GatherMapsResult,
      leftTable: Table,
      rightTable: Table,
      condition: LazyCompiledCondition): GatherMapsResult = {
    val compiledCondition = condition.getForBuildSide(backend.buildSide)
    if (backend.buildSide == GpuBuildLeft) {
      JoinImpl.filterInnerJoinWithSwappedAST(
        innerMaps, leftTable, rightTable, compiledCondition)
    } else {
      JoinImpl.filterInnerJoinWithAST(innerMaps, leftTable, rightTable, compiledCondition)
    }
  }

  override def close(): Unit = backend.close()
}

/**
 * The physical build implementation of a hash probe selected once for a stream batch.
 * The caller owns the backend until `close()`.
 */
sealed trait HashProbeBackend extends AutoCloseable {
  def buildSide: GpuBuildSide
  def isCached: Boolean

  def execute(
      primitive: HashJoinPrimitive,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long] = None): GatherMapsResult

  /** Exact output size when this backend can reuse its build artifact to compute it. */
  def outputRowCount(joinType: JoinType, probeKeys: Table): Option[Long]
}

/**
 * Ephemeral backend that executes directly from the two key tables without retaining a native
 * artifact. It cannot provide an exact count ahead of execution and has no resources to close.
 */
private final class OnDemandHashProbeBackend(
    override val buildSide: GpuBuildSide,
    compareNullsEqual: Boolean) extends HashProbeBackend {
  override val isCached: Boolean = false

  override def execute(
      primitive: HashJoinPrimitive,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = primitive match {
    case HashJoinPrimitive.Inner => inner(leftKeys, rightKeys)
    case HashJoinPrimitive.LeftOuter => leftOuter(leftKeys, rightKeys)
    case HashJoinPrimitive.RightOuter => rightOuter(leftKeys, rightKeys)
    case HashJoinPrimitive.LeftSemi => leftSemi(leftKeys, rightKeys)
    case HashJoinPrimitive.LeftAnti => leftAnti(leftKeys, rightKeys)
    case HashJoinPrimitive.Distinct(joinType, _) => distinct(joinType, leftKeys, rightKeys)
  }

  private def inner(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = buildSide match {
    case GpuBuildLeft => JoinImpl.innerHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
    case GpuBuildRight => JoinImpl.innerHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
  }

  private def leftOuter(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    JoinImpl.leftOuterHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
  }

  private def rightOuter(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    JoinImpl.rightOuterHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
  }

  private def leftSemi(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    JoinImpl.leftSemiHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
  }

  private def leftAnti(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    JoinImpl.leftAntiHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
  }

  private def distinct(
      joinType: JoinType,
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = joinType match {
    case LeftOuter =>
      GatherMapsResult.makeFromRight(leftKeys.leftDistinctJoinGatherMap(
        rightKeys, compareNullsEqual))
    case RightOuter =>
      GatherMapsResult.makeFromLeft(rightKeys.leftDistinctJoinGatherMap(
        leftKeys, compareNullsEqual))
    case _: InnerLike if buildSide == GpuBuildRight =>
      val maps = leftKeys.innerDistinctJoinGatherMaps(rightKeys, compareNullsEqual)
      GatherMapsResult(maps(0), maps(1))
    case _: InnerLike =>
      val maps = rightKeys.innerDistinctJoinGatherMaps(leftKeys, compareNullsEqual)
      GatherMapsResult(maps(1), maps(0))
    case LeftSemi => leftSemi(leftKeys, rightKeys)
    case LeftAnti => leftAnti(leftKeys, rightKeys)
    case _ => throw new IllegalStateException(s"unsupported distinct join type $joinType")
  }

  override def outputRowCount(joinType: JoinType, probeKeys: Table): Option[Long] = None

  override def close(): Unit = {}
}

/**
 * Cache-owned native hash state that can provide leased probe backends.
 * Implementations retain immutable build statistics and a [[SharedRecomputableHandle]]. Calling
 * `backend` acquires a lease and transfers ownership of that lease to the returned backend.
 */
private[execution] trait HashArtifact extends AutoCloseable {
  def stats: JoinBuildSideStats
  def isReady: Boolean
  def backend(
      buildSide: GpuBuildSide,
      metrics: HashBuildMetrics,
      onRebuild: () => Unit): HashProbeBackend
}

/** Recomputable non-distinct cuDF hash table owned by a [[HashBuildCache]]. */
private final class HashJoinArtifact(
    override val stats: JoinBuildSideStats,
    handle: SharedRecomputableHandle[CudfHashJoin]) extends HashArtifact {
  override def isReady: Boolean = handle.isReady

  override def backend(
      physicalBuildSide: GpuBuildSide,
      metrics: HashBuildMetrics,
      onRebuild: () => Unit): HashProbeBackend = {
    val lease = handle.acquire()
    closeOnExcept(lease) { _ =>
      if (lease.rebuilt) {
        metrics.rebuilds += 1
        onRebuild()
      }
      new CachedHashProbeBackend(physicalBuildSide, Left(lease))
    }
  }

  override def close(): Unit = handle.close()
}

/** Recomputable cuDF distinct hash table owned by a [[HashBuildCache]]. */
private final class DistinctHashJoinArtifact(
    override val stats: JoinBuildSideStats,
    handle: SharedRecomputableHandle[CudfDistinctHashJoin]) extends HashArtifact {
  override def isReady: Boolean = handle.isReady

  override def backend(
      physicalBuildSide: GpuBuildSide,
      metrics: HashBuildMetrics,
      onRebuild: () => Unit): HashProbeBackend = {
    val lease = handle.acquire()
    closeOnExcept(lease) { _ =>
      if (lease.rebuilt) {
        metrics.rebuilds += 1
        onRebuild()
      }
      new CachedHashProbeBackend(physicalBuildSide, Right(lease))
    }
  }

  override def close(): Unit = handle.close()
}

/**
 * Backend that holds a lease on a reusable cuDF hash artifact until `close()`. This probes a
 * pre-built native table and can compute exact inner/outer counts for non-distinct artifacts.
 */
private final class CachedHashProbeBackend(
    override val buildSide: GpuBuildSide,
    artifact: Either[SharedRecomputableHandle.Lease[CudfHashJoin],
      SharedRecomputableHandle.Lease[CudfDistinctHashJoin]]) extends HashProbeBackend {
  override val isCached: Boolean = true

  private def withHashJoin[T](f: CudfHashJoin => T): T = artifact match {
    case Left(lease) => f(lease.resource)
    case Right(_) => throw new IllegalStateException("expected a non-distinct hash build")
  }

  private def withDistinctHashJoin[T](f: CudfDistinctHashJoin => T): T = artifact match {
    case Right(lease) => f(lease.resource)
    case Left(_) => throw new IllegalStateException("expected a distinct hash build")
  }

  override def execute(
      primitive: HashJoinPrimitive,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = primitive match {
    case HashJoinPrimitive.Inner => inner(leftKeys, rightKeys, outputRowCount)
    case HashJoinPrimitive.LeftOuter => leftOuter(leftKeys, rightKeys, outputRowCount)
    case HashJoinPrimitive.RightOuter => rightOuter(leftKeys, rightKeys, outputRowCount)
    case HashJoinPrimitive.LeftSemi => leftSemi(leftKeys, rightKeys)
    case HashJoinPrimitive.LeftAnti => leftAnti(leftKeys, rightKeys)
    case HashJoinPrimitive.Distinct(joinType, _) => distinct(joinType, leftKeys, rightKeys)
  }

  private def inner(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long] = None): GatherMapsResult = artifact match {
    case Left(_) => withHashJoin { hashJoin =>
      buildSide match {
        case GpuBuildLeft =>
          JoinImpl.innerHashJoinBuildLeft(rightKeys, hashJoin, outputRowCount)
        case GpuBuildRight =>
          JoinImpl.innerHashJoinBuildRight(leftKeys, hashJoin, outputRowCount)
      }
    }
    case Right(_) => withDistinctHashJoin { distinctHashJoin =>
      buildSide match {
        case GpuBuildLeft =>
          JoinImpl.innerDistinctHashJoinBuildLeft(rightKeys, distinctHashJoin)
        case GpuBuildRight =>
          JoinImpl.innerDistinctHashJoinBuildRight(leftKeys, distinctHashJoin)
      }
    }
  }

  private def leftOuter(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = withHashJoin { hashJoin =>
    require(buildSide == GpuBuildRight, "left outer joins must build the right side")
    JoinImpl.leftOuterHashJoinBuildRight(leftKeys, hashJoin, outputRowCount)
  }

  private def rightOuter(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = withHashJoin { hashJoin =>
    require(buildSide == GpuBuildLeft, "right outer joins must build the left side")
    JoinImpl.rightOuterHashJoinBuildLeft(rightKeys, hashJoin, outputRowCount)
  }

  private def leftSemi(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    require(buildSide == GpuBuildRight, "left semi joins must build the right side")
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftSemi(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  private def leftAnti(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    require(buildSide == GpuBuildRight, "left anti joins must build the right side")
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftAnti(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  private def distinct(
      joinType: JoinType,
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    joinType match {
      case LeftOuter | LeftSemi | LeftAnti =>
        require(buildSide == GpuBuildRight, s"$joinType joins must build the right side")
      case RightOuter =>
        require(buildSide == GpuBuildLeft, "right outer joins must build the left side")
      case _ =>
    }
    withDistinctHashJoin { distinctHashJoin =>
      joinType match {
        case LeftOuter =>
          JoinImpl.leftOuterDistinctHashJoinBuildRight(leftKeys, distinctHashJoin)
        case RightOuter =>
          JoinImpl.rightOuterDistinctHashJoinBuildLeft(rightKeys, distinctHashJoin)
        case LeftSemi =>
          withResource(JoinImpl.innerDistinctHashJoinBuildRight(leftKeys, distinctHashJoin)) {
            innerMaps => JoinImpl.makeLeftSemi(innerMaps, leftKeys.getRowCount.toInt)
          }
        case LeftAnti =>
          withResource(JoinImpl.innerDistinctHashJoinBuildRight(leftKeys, distinctHashJoin)) {
            innerMaps => JoinImpl.makeLeftAnti(innerMaps, leftKeys.getRowCount.toInt)
          }
        case _: InnerLike if buildSide == GpuBuildRight =>
          JoinImpl.innerDistinctHashJoinBuildRight(leftKeys, distinctHashJoin)
        case _: InnerLike =>
          JoinImpl.innerDistinctHashJoinBuildLeft(rightKeys, distinctHashJoin)
        case _ => throw new IllegalStateException(s"unsupported distinct join type $joinType")
      }
    }
  }

  override def outputRowCount(joinType: JoinType, probeKeys: Table): Option[Long] = {
    artifact match {
      case Left(_) => Some(withHashJoin { hashJoin =>
        joinType match {
          case _: InnerLike => probeKeys.innerJoinRowCount(hashJoin)
          case LeftOuter | RightOuter => probeKeys.leftJoinRowCount(hashJoin)
          case _ => throw new IllegalStateException(
            s"exact output row count is unsupported for $joinType")
        }
      })
      case Right(_) => None
    }
  }

  override def close(): Unit = artifact.fold(_.close(), _.close())
}

/**
 * Executor-local cache of reusable hash-build artifacts.
 *
 * The cache stores [[HashArtifact]] objects accessed by [[HashBuildKey]]. Upon the first get
 * the cache ensures one builder at a time while other threads wait. The build produces a
 * [[SharedRecomputableHandle]] that is held by the constructed HashArtifact.
 *
 * Additionally the cache tracks `demand`, a count of the number of probes and total probe rows
 * for a given hash build that has not been cached yet, tracked via `observeProbe`. This is used
 * for heuristics to decide when to cache a hash build based on the cost of the probes we have
 * seen in a stream so far.
 */
final class HashBuildCache extends AutoCloseable {
  private[this] val builds = mutable.HashMap.empty[HashBuildKey, CompletableFuture[HashArtifact]]
  private[this] val demands = mutable.HashMap.empty[(HashBuildKey, HashBuildDemandId), BuildDemand]
  private[this] var closed = false

  private def futureFor(key: HashBuildKey): Option[CompletableFuture[HashArtifact]] = synchronized {
    builds.get(key)
  }

  /**
   * Return a snapshot for `key`:
   *  - `Cold` means no usable cache entry
   *  - `Bulding` means initial construction is in flight
   *  - `Ready` means the native artifact is resident
   *  - `Evicted` means the cached handle exists but must be rebuilt on the next acquisition
   */
  def status(key: HashBuildKey): BuildStatus = futureFor(key) match {
    case None => BuildStatus.Cold
    case Some(future) if !future.isDone => BuildStatus.Building
    case Some(future) if future.isCompletedExceptionally => BuildStatus.Cold
    case Some(future) =>
      if (future.getNow(null).isReady) BuildStatus.Ready else BuildStatus.Evicted
  }

  /** Return build statistics for a `key` only if it has a resident artifact. */
  def readyStats(key: HashBuildKey): Option[JoinBuildSideStats] = futureFor(key).flatMap { future =>
    if (future.isDone && !future.isCompletedExceptionally) {
      val artifact = future.getNow(null)
      if (artifact.isReady) Some(artifact.stats) else None
    } else {
      None
    }
  }

  /** Return accumulated probe demand for this key and demand scope, or an empty value if unseen. */
  private[execution] def demand(
      key: HashBuildKey,
      demandId: HashBuildDemandId): BuildDemand = synchronized {
    demands.getOrElse((key, demandId), BuildDemand.Empty)
  }

  /**
   * Atomically records non-empty probe demand for one join/stage attempt. Demand is kept separate
   * across attempts and reset once the reusable artifact has been successfully built.
   */
  private[execution] def observeProbe(
      key: HashBuildKey,
      demandId: HashBuildDemandId,
      probeRows: Long): BuildDemand = {
    require(probeRows >= 0, s"invalid probe row count $probeRows")
    synchronized {
      val demandKey = (key, demandId)
      if (probeRows == 0) {
        demands.getOrElse(demandKey, BuildDemand.Empty)
      } else {
        val previous = demands.getOrElse(demandKey, BuildDemand.Empty)
        val updated = BuildDemand(previous.probeCount + 1L, previous.probeRows + probeRows)
        demands.put(demandKey, updated)
        updated
      }
    }
  }

  /** Clear demand for all join/stage-attempt scopes associated with `key`. */
  private[execution] def resetDemands(key: HashBuildKey): Unit = synchronized {
    demands.keysIterator.filter(_._1 == key).toSeq.foreach(demands.remove)
  }

  /**
   * Returns the artifact for `key` and whether it was reused, with single-flight construction
   * for concurrent callers. The winning caller installs the future under the lock, and then
   * completes the future outside the lock, while others wait. If construction fails the
   * entry is removed so a later caller can retry.
   *
   * @param key the HashBuildKey uniquely describing the particular build artifact
   * @param metrics the hash build metrics to update
   * @param create a by-name parameter that creates the GPU hash artifact
   * @return the artifact, and whether an existing entry was reused
   */
  private[execution] def getOrBuild(
      key: HashBuildKey,
      metrics: HashBuildMetrics)(create: => HashArtifact): (HashArtifact, Boolean) = {
    // All threads decide whether to build or wait under the lock.
    val (future, shouldBuild) = synchronized {
      if (closed) {
        throw new IllegalStateException("attempting to use a closed hash-build cache")
      }
      builds.get(key) match {
        case Some(existing) => (existing, false)
        case None =>
          // Publish a placeholder under the monitor to indicate that this thread builds.
          val pending = new CompletableFuture[HashArtifact]()
          builds.put(key, pending)
          (pending, true)
      }
    }

    if (shouldBuild) {
      try {
        // Call create to build the artifact, and publish it to the future if successful.
        closeOnExcept(create) { artifact =>
          val keepArtifact = synchronized {
            if (closed) {
              false
            } else {
              metrics.builds += 1
              resetDemands(key)
              future.complete(artifact)
            }
          }
          if (!keepArtifact) {
            val failure = new IllegalStateException("hash-build cache closed during build")
            future.completeExceptionally(failure)
            throw failure
          }
        }
      } catch {
        case t: Throwable =>
          synchronized {
            builds.remove(key)
            future.completeExceptionally(t)
          }
          throw t
      }
    }

    val taskContext = TaskContext.get()
    val shouldReleaseSemaphore = !future.isDone
    if (shouldReleaseSemaphore) {
      // The winning builder may need the GPU semaphore currently held by this waiting task.
      GpuSemaphore.releaseIfNecessary(taskContext)
    }
    val artifact = try {
      future.get()
    } catch {
      case e: ExecutionException => throw e.getCause
    } finally {
      if (shouldReleaseSemaphore) {
        GpuSemaphore.acquireIfNecessary(taskContext)
      }
    }
    (artifact, !shouldBuild)
  }

  override def close(): Unit = {
    // Mark the cache as closed and clear both maps.
    val futures = synchronized {
      closed = true
      val pending = builds.values.toSeq
      builds.clear()
      demands.clear()
      pending
    }
    var interrupted = false
    val artifacts = mutable.ArrayBuffer.empty[HashArtifact]
    // Release the semaphore if a build is pending, before we wait on futures.
    val taskContext = TaskContext.get()
    val shouldReleaseSemaphore = futures.exists(!_.isDone)
    if (shouldReleaseSemaphore) {
      GpuSemaphore.releaseIfNecessary(taskContext)
    }
    // Wait for futures to complete and then close the completed results so that an
    // artifact isn't abandoned after the cache is closed.
    try {
      futures.foreach { future =>
        var waiting = true
        while (waiting) {
          try {
            artifacts += future.get()
            waiting = false
          } catch {
            case _: ExecutionException => waiting = false
            case _: InterruptedException => interrupted = true
          }
        }
      }
    } finally {
      try {
        if (shouldReleaseSemaphore) {
          GpuSemaphore.acquireIfNecessary(taskContext)
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt()
        }
      }
    }
    artifacts.safeClose()
  }
}

/**
 * Provider of [[HashProbeBackend]]. At the start of the join, the join iterator creates one
 * provider before processing stream batches. For each stream batch, it asks the provider to decide
 * which backend to use (describing which side to build and whether to reuse an artifact or build
 * on-demand).
 */
sealed trait HashBackendProvider {
  def readyStats: Option[JoinBuildSideStats]

  def backend(
      probe: HashProbeContext,
      primitive: HashJoinPrimitive): HashProbeBackend
}

/** Per-batch inputs used to select a physical hash-build implementation. */
case class HashProbeContext(
    selection: JoinBuildSideSelection.JoinBuildSideSelection,
    planBuildSide: GpuBuildSide,
    leftRowCount: Long,
    rightRowCount: Long,
    compareNullsEqual: Boolean)

private[execution] final class CachedHashBuildUnavailable(cause: Throwable)
    extends RuntimeException("reusable hash build is unavailable", cause)

/** Default provider that creates a fresh on-demand backend for every stream batch. */
object OnDemandHashBackendProvider extends HashBackendProvider {
  override val readyStats: Option[JoinBuildSideStats] = None

  override def backend(
      probe: HashProbeContext,
      primitive: HashJoinPrimitive): HashProbeBackend = {
    val side = primitive.requiredBuildSide.getOrElse(
      JoinBuildSideSelection.selectPhysicalBuildSide(
        probe.selection,
        probe.planBuildSide,
        probe.leftRowCount,
        probe.rightRowCount))
    new OnDemandHashProbeBackend(side, probe.compareNullsEqual)
  }
}

/**
 * Selects between a reusable build and the ordinary on-demand backend for each probe batch.
 * This provider holds the reusable build's cache identity, demand scope, and deferred
 * construction function. The [[HashBuildCache]] owns any constructed artifacts.
 * The `offeredSide` is the side that this provider can construct and reuse. E.g. for
 * broadcast joins it is the plan's broadcast build side.
 */
final class CachedHashBackendProvider private[execution] (
    offeredSide: GpuBuildSide,
    numericKeys: Boolean,
    demandId: HashBuildDemandId,
    cache: HashBuildCache,
    key: HashBuildKey,
    create: () => HashArtifact,
    metrics: HashBuildMetrics) extends HashBackendProvider {
  override def readyStats: Option[JoinBuildSideStats] = cache.readyStats(key)

  /**
   * Resolve the backend for one probe batch. Resolution is as follows:
   * - if the primitive has a required build side, that side takes precedence
   * - otherwise if build side selection is AUTO, record demand and defer to [[HashBuildPlanner]]
   * Selecting the offered side lazily constructs or acquires its cached artifact; selecting
   * the other side returns an on-demand backend.
   */
  override def backend(
      probe: HashProbeContext,
      primitive: HashJoinPrimitive): HashProbeBackend = {
    // If the primitive has a required build side, bypass policy selection.
    val selectedSide = primitive.requiredBuildSide.getOrElse {
      val observedStatus = cache.status(key)
      val probeRows = if (offeredSide == GpuBuildLeft) {
        probe.rightRowCount
      } else {
        probe.leftRowCount
      }
      // Record the probe demand if the build cache is cold or evicted. Otherwise,
      // get the current demand for this hash build.
      val recordDemand = probe.selection == JoinBuildSideSelection.AUTO &&
        (observedStatus == BuildStatus.Cold || observedStatus == BuildStatus.Evicted)
      val demand = if (recordDemand) {
        cache.observeProbe(key, demandId, probeRows)
      } else {
        cache.demand(key, demandId)
      }
      // Re-read after recording rent in case a build was just published.
      val currentStatus = cache.status(key)
      // Pass the probe context and the accumulated demand for this build to the
      // planner and let it select the build side to use.
      HashBuildPlanner.select(
        probe.selection,
        probe.planBuildSide,
        probe.leftRowCount,
        probe.rightRowCount,
        offeredSide,
        currentStatus,
        numericKeys,
        demand)
    }
    if (selectedSide == offeredSide) {
      try {
        acquireCachedBackend(selectedSide)
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw e
        case e: OutOfMemoryError => throw new CachedHashBuildUnavailable(e)
        case NonFatal(e) => throw new CachedHashBuildUnavailable(e)
      }
    } else {
      new OnDemandHashProbeBackend(selectedSide, probe.compareNullsEqual)
    }
  }

  private def acquireCachedBackend(buildSide: GpuBuildSide): HashProbeBackend = {
    val (artifact, reused) = cache.getOrBuild(key, metrics)(create())
    val backend = artifact.backend(
      buildSide, metrics, () => cache.resetDemands(key))
    closeOnExcept(backend) { _ =>
      if (reused) {
        metrics.reuses += 1
      }
      backend
    }
  }
}

/** Builds a native hash artifact from a source-owned batch. */
object HashBuildFactory {
  private def withBuildKeys[T](
      buildBatch: SpillableColumnarBatch,
      boundKeys: Seq[GpuExpression],
      filterOutNulls: Boolean,
      prepareBatch: Option[ColumnarBatch => ColumnarBatch])(f: Table => T): T = {
    def projectAndApply(cb: ColumnarBatch): T = {
      withResource(GpuProjectExec.project(cb, boundKeys)) { buildKeys =>
        withResource(GpuColumnVector.from(buildKeys)) { keys =>
          f(keys)
        }
      }
    }

    def filterAndApply(cb: ColumnarBatch): T = {
      if (filterOutNulls) {
        val retained = GpuColumnVector.incRefCounts(cb)
        val spillable = closeOnExcept(retained) { batch =>
          SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        withResource(GpuHashJoin.filterNullsWithRetryAndClose(spillable, boundKeys)) {
          projectAndApply
        }
      } else {
        projectAndApply(cb)
      }
    }

    def prepareAndApply(cb: ColumnarBatch): T = prepareBatch match {
      case Some(prepare) =>
        withResource(prepare(GpuColumnVector.incRefCounts(cb)))(filterAndApply)
      case None => filterAndApply(cb)
    }

    val retainedBatch = buildBatch.incRefCount()
    withRetryNoSplit(retainedBatch) { attempt =>
      withResource(attempt.getColumnarBatch())(prepareAndApply)
    }
  }

  /** Build the initial native artifact and capture the recipe needed to rebuild it. */
  private[execution] def create(
      buildBatch: SpillableColumnarBatch,
      boundKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean,
      prepareBatch: Option[ColumnarBatch => ColumnarBatch]): HashArtifact = {
    withBuildKeys(buildBatch, boundKeys, filterOutNulls, prepareBatch) { keys =>
      val stats = JoinBuildSideStats.fromTable(keys)
      // cuDF does not expose the hash-table size, so use the projected key size for accounting.
      val approxSizeInBytes = keys.getDeviceMemorySize
      if (stats.isDistinct) {
        new DistinctHashJoinArtifact(
          stats,
          SharedRecomputableHandle(
            approxSizeInBytes,
            NvtxRegistry.HASH_TABLE_BUILD {
              new CudfDistinctHashJoin(keys, compareNullsEqual)
            }) {
            withBuildKeys(buildBatch, boundKeys, filterOutNulls, prepareBatch) { rebuiltKeys =>
              NvtxRegistry.HASH_TABLE_BUILD {
                new CudfDistinctHashJoin(rebuiltKeys, compareNullsEqual)
              }
            }
          })
      } else {
        new HashJoinArtifact(
          stats,
          SharedRecomputableHandle(
            approxSizeInBytes,
            NvtxRegistry.HASH_TABLE_BUILD {
              new CudfHashJoin(keys, compareNullsEqual)
            }) {
            withBuildKeys(buildBatch, boundKeys, filterOutNulls, prepareBatch) { rebuiltKeys =>
              NvtxRegistry.HASH_TABLE_BUILD {
                new CudfHashJoin(rebuiltKeys, compareNullsEqual)
              }
            }
          })
      }
    }
  }
}
