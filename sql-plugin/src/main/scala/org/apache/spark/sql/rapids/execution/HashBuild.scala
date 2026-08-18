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
import com.nvidia.spark.rapids.spill.{RecomputableHandle, SharedRecomputableHandle}

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
 * Identifies derived hash-build state within a cache.
 *
 * A cache owner can contain multiple projections of the same data, so the projection type and
 * nullability are included along with its canonical expression.
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

/** The cuDF hash primitive selected for one stream batch. */
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

  def direct(joinType: JoinType): HashJoinPrimitive = joinType match {
    case _: InnerLike => Inner
    case org.apache.spark.sql.catalyst.plans.LeftOuter => LeftOuter
    case org.apache.spark.sql.catalyst.plans.RightOuter => RightOuter
    case org.apache.spark.sql.catalyst.plans.LeftSemi => LeftSemi
    case org.apache.spark.sql.catalyst.plans.LeftAnti => LeftAnti
    case other => throw new IllegalStateException(s"unsupported hash join primitive $other")
  }
}

/** A hash primitive, backend, and exact primitive count resolved once for a stream batch. */
private[execution] final class ResolvedHashJoin(
    private val backend: HashProbeBackend,
    primitive: HashJoinPrimitive,
    val exactOutputRows: Option[Long]) extends AutoCloseable {
  def isCached: Boolean = backend.isCached

  def execute(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    backend.execute(primitive, leftKeys, rightKeys, exactOutputRows)
  }

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

/** A build implementation selected once for a probe operation. */
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

private[execution] trait HashArtifact extends AutoCloseable {
  def stats: JoinBuildSideStats
  def isReady: Boolean
  def backend(
      buildSide: GpuBuildSide,
      metrics: HashBuildMetrics,
      onRebuild: () => Unit): HashProbeBackend
}

private[execution] case class HashArtifactLookup(artifact: HashArtifact, reused: Boolean)

private final class HashJoinArtifact(
    override val stats: JoinBuildSideStats,
    handle: RecomputableHandle[CudfHashJoin]) extends HashArtifact {
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

private final class DistinctHashJoinArtifact(
    override val stats: JoinBuildSideStats,
    handle: RecomputableHandle[CudfDistinctHashJoin]) extends HashArtifact {
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

private final class CachedHashProbeBackend(
    override val buildSide: GpuBuildSide,
    artifact: Either[RecomputableHandle.Lease[CudfHashJoin],
      RecomputableHandle.Lease[CudfDistinctHashJoin]]) extends HashProbeBackend {
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
 * All mutable cache and demand state is guarded by this object's monitor. Initial construction is
 * single-flight per key: the first caller publishes an incomplete future under the monitor, builds
 * outside it, and completes the future when the artifact is ready. Concurrent callers wait on that
 * future without holding either this monitor or the GPU semaphore. Once published, the artifact's
 * [[RecomputableHandle]] independently coordinates leases, spill, and post-eviction rebuilds.
 */
final class HashBuildCache extends AutoCloseable {
  private[this] val builds = mutable.HashMap.empty[HashBuildKey,
    CompletableFuture[HashArtifact]]
  private[this] val demands = mutable.HashMap.empty[(HashBuildKey, HashBuildDemandId), BuildDemand]
  private[this] var closed = false

  // This is only a retention bound to keep metadata from growing indefinitely. Reaching it
  // requires the same hash-build key to be used by this many distinct join/stage attempts without
  // a successful build clearing demand, at which point we start dropping old entries.
  private[this] val maxTrackedDemandIdsPerKey = 100

  private def saturatedAdd(left: Long, right: Long): Long = {
    if (right > Long.MaxValue - left) Long.MaxValue else left + right
  }

  private def futureFor(key: HashBuildKey): Option[CompletableFuture[HashArtifact]] = synchronized {
    builds.get(key)
  }

  def status(key: HashBuildKey): BuildStatus = futureFor(key) match {
    case None => BuildStatus.Cold
    case Some(future) if !future.isDone => BuildStatus.Building
    case Some(future) if future.isCompletedExceptionally => BuildStatus.Cold
    case Some(future) =>
      if (future.getNow(null).isReady) BuildStatus.Ready else BuildStatus.Evicted
  }

  def readyStats(key: HashBuildKey): Option[JoinBuildSideStats] = futureFor(key).flatMap { future =>
    if (future.isDone && !future.isCompletedExceptionally) {
      val artifact = future.getNow(null)
      if (artifact.isReady) Some(artifact.stats) else None
    } else {
      None
    }
  }

  private[execution] def demand(
      key: HashBuildKey,
      demandId: HashBuildDemandId): BuildDemand = synchronized {
    demands.getOrElse((key, demandId), BuildDemand.Empty)
  }

  /**
   * Atomically records non-empty probe demand for one join/stage attempt. Demand is kept separate
   * across attempts, bounded to avoid retaining stale generations, and reset once the corresponding
   * reusable artifact has been successfully built.
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
        if (!demands.contains(demandKey)) {
          val generations = demands.keysIterator.filter(_._1 == key).toSeq
          if (generations.size >= maxTrackedDemandIdsPerKey) {
            val oldest = generations.minBy { case (_, id) =>
              (id.stageId, id.stageAttempt, id.joinId)
            }
            demands.remove(oldest)
          }
        }
        val previous = demands.getOrElse(demandKey, BuildDemand.Empty)
        val updated = BuildDemand(
          saturatedAdd(previous.probeCount, 1L),
          saturatedAdd(previous.probeRows, probeRows))
        demands.put(demandKey, updated)
        updated
      }
    }
  }

  private[execution] def resetDemands(key: HashBuildKey): Unit = synchronized {
    demands.keysIterator.filter(_._1 == key).toSeq.foreach(demands.remove)
  }

  /**
   * Returns the artifact lookup for `key`, constructing it exactly once for concurrent callers.
   *
   * The winning caller installs the future while synchronized and performs the expensive GPU build
   * outside the monitor. Other callers share the future and release the GPU semaphore before
   * waiting so the builder can make progress. A failed build completes all waiters exceptionally
   * and removes the entry so a later caller may retry. The lookup records whether this caller
   * reused an existing entry so reuse is counted only after its native lease is acquired. If the
   * cache closes during construction, the newly built artifact is discarded instead of being
   * published.
   */
  private[execution] def getOrBuild(
      key: HashBuildKey,
      metrics: HashBuildMetrics)(create: => HashArtifact): HashArtifactLookup = {
    val (future, shouldBuild) = synchronized {
      if (closed) {
        throw new IllegalStateException("attempting to use a closed hash-build cache")
      }
      builds.get(key) match {
        case Some(existing) => (existing, false)
        case None =>
          // Publish the placeholder before leaving the monitor so no second caller can build the
          // same key while construction is in flight.
          val pending = new CompletableFuture[HashArtifact]()
          builds.put(key, pending)
          (pending, true)
      }
    }

    if (shouldBuild) {
      // GPU construction can be slow and may itself need shared executor resources, so never run it
      // while holding the cache monitor.
      try {
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
    HashArtifactLookup(artifact, reused = !shouldBuild)
  }

  private[execution] def offer(
      side: GpuBuildSide,
      numericKeys: Boolean,
      demandId: HashBuildDemandId,
      key: HashBuildKey,
      create: () => HashArtifact): HashBuildCacheEntry = {
    new HashBuildCacheEntry(side, numericKeys, demandId, this, key, create)
  }

  override def close(): Unit = {
    val futures = synchronized {
      closed = true
      val pending = builds.values.toSeq
      builds.clear()
      demands.clear()
      pending
    }
    var interrupted = false
    val artifacts = mutable.ArrayBuffer.empty[HashArtifact]
    val taskContext = TaskContext.get()
    val shouldReleaseSemaphore = futures.exists(!_.isDone)
    if (shouldReleaseSemaphore) {
      GpuSemaphore.releaseIfNecessary(taskContext)
    }
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

/** A source-owned view of one reusable hash-build cache entry. */
final class HashBuildCacheEntry private[execution] (
    val side: GpuBuildSide,
    private[execution] val numericKeys: Boolean,
    demandId: HashBuildDemandId,
    cache: HashBuildCache,
    key: HashBuildKey,
    create: () => HashArtifact) {
  def status: BuildStatus = cache.status(key)
  def readyStats: Option[JoinBuildSideStats] = cache.readyStats(key)
  private[execution] def demand: BuildDemand = cache.demand(key, demandId)
  private[execution] def observeProbe(probeRows: Long): BuildDemand = {
    cache.observeProbe(key, demandId, probeRows)
  }
  private[execution] def acquireBackend(
      buildSide: GpuBuildSide,
      metrics: HashBuildMetrics,
      onRebuild: () => Unit): HashProbeBackend = {
    val lookup = cache.getOrBuild(key, metrics)(create())
    val backend = lookup.artifact.backend(buildSide, metrics, onRebuild)
    closeOnExcept(backend) { _ =>
      if (lookup.reused) {
        metrics.reuses += 1
      }
      backend
    }
  }
  private[execution] def resetDemand(): Unit = cache.resetDemands(key)
}

/**
 * Pluggable provider of hash-probe backends. Join iterators ask it to resolve the backend for the
 * current probe without branching on cache availability.
 */
sealed trait HashBackendProvider {
  def readyStats: Option[JoinBuildSideStats]

  def backend(
      probe: HashProbeContext,
      primitive: HashJoinPrimitive): HashProbeBackend
}

/** All per-batch inputs needed to select a physical hash-build implementation. */
case class HashProbeContext(
    selection: JoinBuildSideSelection.JoinBuildSideSelection,
    planBuildSide: GpuBuildSide,
    leftRowCount: Long,
    rightRowCount: Long,
    compareNullsEqual: Boolean)

private[execution] final class CachedHashBuildUnavailable(cause: Throwable)
    extends RuntimeException("reusable hash build is unavailable", cause)

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

final class CachedHashBackendProvider(
    entry: HashBuildCacheEntry,
    metrics: HashBuildMetrics) extends HashBackendProvider {
  override def readyStats: Option[JoinBuildSideStats] = entry.readyStats

  override def backend(
      probe: HashProbeContext,
      primitive: HashJoinPrimitive): HashProbeBackend = {
    val selectedSide = primitive.requiredBuildSide.getOrElse {
      val observedStatus = entry.status
      val probeRows = if (entry.side == GpuBuildLeft) {
        probe.rightRowCount
      } else {
        probe.leftRowCount
      }
      val shouldRent = probe.selection == JoinBuildSideSelection.AUTO &&
        (observedStatus == BuildStatus.Cold || observedStatus == BuildStatus.Evicted)
      val demand = if (shouldRent) {
        entry.observeProbe(probeRows)
      } else {
        entry.demand
      }
      // Re-read after recording rent so a concurrent build publication is treated as a sunk cost.
      val currentStatus = entry.status
      HashBuildPlanner.select(
        probe.selection,
        probe.planBuildSide,
        probe.leftRowCount,
        probe.rightRowCount,
        entry.side,
        currentStatus,
        entry.numericKeys,
        demand)
    }
    if (selectedSide == entry.side) {
      try {
        entry.acquireBackend(selectedSide, metrics, () => entry.resetDemand())
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
}

/** Builds the native artifact represented by a source-owned cache entry. */
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
