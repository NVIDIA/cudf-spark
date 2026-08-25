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
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
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
 * A request to execute one equi-hash join operation through [[HashProbeBackend]].
 * `requiredBuildSide` specifies whether the requested operation has a physical side constraint,
 * e.g. left or right outer/semi/anti or a known-distinct build side. `exactCountJoinType`
 * specifies when the backend can provide an exact output count before execution, and which
 * JoinType requests that count.
 */
sealed trait BackendJoinRequest {
  def requiredBuildSide: Option[GpuBuildSide]
  def exactCountJoinType: Option[JoinType]
}

private[execution] object BackendJoinRequest {
  case object Inner extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = None
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.Inner)
  }

  case object LeftOuter extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.LeftOuter)
  }

  case object RightOuter extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildLeft)
    override val exactCountJoinType: Option[JoinType] =
      Some(org.apache.spark.sql.catalyst.plans.RightOuter)
  }

  case object LeftSemi extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  case object LeftAnti extends BackendJoinRequest {
    override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    override val exactCountJoinType: Option[JoinType] = None
  }

  /** Request that executes using a build side whose join keys are known to be distinct. */
  sealed trait Distinct extends BackendJoinRequest {
    final override val exactCountJoinType: Option[JoinType] = None
  }

  object Distinct {
    final case class Inner(buildSide: GpuBuildSide) extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(buildSide)
    }

    case object LeftOuter extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    case object RightOuter extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildLeft)
    }

    case object LeftSemi extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    case object LeftAnti extends Distinct {
      override val requiredBuildSide: Option[GpuBuildSide] = Some(GpuBuildRight)
    }

    /** Map a Catalyst join type and its known-distinct build side to a backend request. */
    def apply(joinType: JoinType, planBuildSide: GpuBuildSide): Distinct = {
      val request = joinType match {
        case _: InnerLike => Inner(planBuildSide)
        case org.apache.spark.sql.catalyst.plans.LeftOuter => LeftOuter
        case org.apache.spark.sql.catalyst.plans.RightOuter => RightOuter
        case org.apache.spark.sql.catalyst.plans.LeftSemi => LeftSemi
        case org.apache.spark.sql.catalyst.plans.LeftAnti => LeftAnti
        case other =>
          throw new IllegalStateException(s"unsupported distinct backend join request $other")
      }
      require(request.requiredBuildSide.contains(planBuildSide),
        s"$joinType distinct join does not support build side $planBuildSide")
      request
    }
  }

  /** Map a directly executed Catalyst join type to its backend request. */
  def direct(joinType: JoinType): BackendJoinRequest = joinType match {
    case _: InnerLike => Inner
    case org.apache.spark.sql.catalyst.plans.LeftOuter => LeftOuter
    case org.apache.spark.sql.catalyst.plans.RightOuter => RightOuter
    case org.apache.spark.sql.catalyst.plans.LeftSemi => LeftSemi
    case org.apache.spark.sql.catalyst.plans.LeftAnti => LeftAnti
    case other => throw new IllegalStateException(s"unsupported backend join request $other")
  }
}

/**
 * A hash operation resolved once for a stream batch. This owns the selected [[HashProbeBackend]].
 */
private[execution] final class ResolvedHashJoin(
    private val backend: HashProbeBackend,
    request: BackendJoinRequest,
    val exactOutputRows: Option[Long]) extends AutoCloseable {
  def isCached: Boolean = backend.isCached

  def execute(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    backend.execute(request, leftKeys, rightKeys, exactOutputRows)
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
 * The caller owns the backend until `close()`. The `buildSide` is the physical side the
 * backend will actually use, under the constraints of `BackendJoinRequest.requiredBuildSide`.
 */
sealed trait HashProbeBackend extends AutoCloseable {
  def buildSide: GpuBuildSide
  def isCached: Boolean

  final def execute(
      request: BackendJoinRequest,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long] = None): GatherMapsResult = {
    request.requiredBuildSide.foreach { requiredSide =>
      require(buildSide == requiredSide,
        s"$request requires build side $requiredSide, but backend builds $buildSide")
    }
    doExecute(request, leftKeys, rightKeys, outputRowCount)
  }

  protected def doExecute(
      request: BackendJoinRequest,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult

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

  override protected def doExecute(
      request: BackendJoinRequest,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = request match {
    case BackendJoinRequest.Inner => inner(leftKeys, rightKeys)
    case BackendJoinRequest.LeftOuter => leftOuter(leftKeys, rightKeys)
    case BackendJoinRequest.RightOuter => rightOuter(leftKeys, rightKeys)
    case BackendJoinRequest.LeftSemi => leftSemi(leftKeys, rightKeys)
    case BackendJoinRequest.LeftAnti => leftAnti(leftKeys, rightKeys)
    case _: BackendJoinRequest.Distinct.Inner => innerDistinct(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.LeftOuter => leftOuterDistinct(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.RightOuter => rightOuterDistinct(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.LeftSemi => leftSemi(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.LeftAnti => leftAnti(leftKeys, rightKeys)
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

  private def innerDistinct(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = buildSide match {
    case GpuBuildLeft =>
      val maps = rightKeys.innerDistinctJoinGatherMaps(leftKeys, compareNullsEqual)
      GatherMapsResult(maps(1), maps(0))
    case GpuBuildRight =>
      val maps = leftKeys.innerDistinctJoinGatherMaps(rightKeys, compareNullsEqual)
      GatherMapsResult(maps(0), maps(1))
  }

  private def leftOuterDistinct(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    GatherMapsResult.makeFromRight(leftKeys.leftDistinctJoinGatherMap(rightKeys, compareNullsEqual))
  }

  private def rightOuterDistinct(
      leftKeys: Table,
      rightKeys: Table): GatherMapsResult = {
    GatherMapsResult.makeFromLeft(rightKeys.leftDistinctJoinGatherMap(leftKeys, compareNullsEqual))
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
      new CachedHashProbeBackend(physicalBuildSide, lease)
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
      new CachedDistinctHashProbeBackend(physicalBuildSide, lease)
    }
  }

  override def close(): Unit = handle.close()
}

/**
 * Backend that holds a lease on a reusable cuDF hash artifact until `close()`. This probes a
 * pre-built native table and can compute exact inner/outer counts.
 */
private final class CachedHashProbeBackend(
    override val buildSide: GpuBuildSide,
    lease: SharedRecomputableHandle.Lease[CudfHashJoin])
    extends HashProbeBackend {
  override val isCached: Boolean = true

  override protected def doExecute(
      request: BackendJoinRequest,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = request match {
    case BackendJoinRequest.Inner => inner(leftKeys, rightKeys, outputRowCount)
    case BackendJoinRequest.LeftOuter => leftOuter(leftKeys, rightKeys, outputRowCount)
    case BackendJoinRequest.RightOuter => rightOuter(leftKeys, rightKeys, outputRowCount)
    case BackendJoinRequest.LeftSemi => leftSemi(leftKeys, rightKeys)
    case BackendJoinRequest.LeftAnti => leftAnti(leftKeys, rightKeys)
    case _: BackendJoinRequest.Distinct =>
      throw new IllegalStateException("expected a distinct hash build")
  }

  private def inner(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long] = None): GatherMapsResult = {
    buildSide match {
      case GpuBuildLeft =>
        JoinImpl.innerHashJoinBuildLeft(rightKeys, lease.resource, outputRowCount)
      case GpuBuildRight =>
        JoinImpl.innerHashJoinBuildRight(leftKeys, lease.resource, outputRowCount)
    }
  }

  private def leftOuter(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = {
    JoinImpl.leftOuterHashJoinBuildRight(leftKeys, lease.resource, outputRowCount)
  }

  private def rightOuter(
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = {
    JoinImpl.rightOuterHashJoinBuildLeft(rightKeys, lease.resource, outputRowCount)
  }

  private def leftSemi(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftSemi(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  private def leftAnti(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftAnti(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  override def outputRowCount(joinType: JoinType, probeKeys: Table): Option[Long] = {
    Some(joinType match {
      case _: InnerLike => probeKeys.innerJoinRowCount(lease.resource)
      case LeftOuter | RightOuter => probeKeys.leftJoinRowCount(lease.resource)
      case _ => throw new IllegalStateException(
        s"exact output row count is unsupported for $joinType")
    })
  }

  override def close(): Unit = lease.close()
}

/**
 * Backend that holds a lease on a reusable cuDF distinct hash artifact until `close()`. This
 * probes a pre-built native table.
 */
private final class CachedDistinctHashProbeBackend(
    override val buildSide: GpuBuildSide,
    lease: SharedRecomputableHandle.Lease[CudfDistinctHashJoin])
    extends HashProbeBackend {
  override val isCached: Boolean = true

  override protected def doExecute(
      request: BackendJoinRequest,
      leftKeys: Table,
      rightKeys: Table,
      outputRowCount: Option[Long]): GatherMapsResult = request match {
    case BackendJoinRequest.Inner => inner(leftKeys, rightKeys)
    case BackendJoinRequest.LeftOuter | BackendJoinRequest.RightOuter =>
      throw new IllegalStateException("expected a non-distinct hash build")
    case BackendJoinRequest.LeftSemi => leftSemi(leftKeys, rightKeys)
    case BackendJoinRequest.LeftAnti => leftAnti(leftKeys, rightKeys)
    case _: BackendJoinRequest.Distinct.Inner => inner(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.LeftOuter => leftOuter(leftKeys)
    case BackendJoinRequest.Distinct.RightOuter => rightOuter(rightKeys)
    case BackendJoinRequest.Distinct.LeftSemi => leftSemi(leftKeys, rightKeys)
    case BackendJoinRequest.Distinct.LeftAnti => leftAnti(leftKeys, rightKeys)
  }

  private def inner(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    buildSide match {
      case GpuBuildLeft =>
        JoinImpl.innerDistinctHashJoinBuildLeft(rightKeys, lease.resource)
      case GpuBuildRight =>
        JoinImpl.innerDistinctHashJoinBuildRight(leftKeys, lease.resource)
    }
  }

  private def leftSemi(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftSemi(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  private def leftAnti(leftKeys: Table, rightKeys: Table): GatherMapsResult = {
    withResource(inner(leftKeys, rightKeys)) { innerMaps =>
      JoinImpl.makeLeftAnti(innerMaps, leftKeys.getRowCount.toInt)
    }
  }

  private def leftOuter(leftKeys: Table): GatherMapsResult = {
    JoinImpl.leftOuterDistinctHashJoinBuildRight(leftKeys, lease.resource)
  }

  private def rightOuter(rightKeys: Table): GatherMapsResult = {
    JoinImpl.rightOuterDistinctHashJoinBuildLeft(rightKeys, lease.resource)
  }

  override def outputRowCount(joinType: JoinType, probeKeys: Table): Option[Long] = None

  override def close(): Unit = lease.close()
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

  /**
   * Return a snapshot for `key` to be used by the planner:
   *  - `Cold` means no usable cache entry for the artifact
   *  - `Building` means initial construction of the artifact is in flight
   *  - `Ready` means the artifact exists and the native resource is resident in the handle
   *  - `Evicted` means the artifact exists but its native resource is not resident
   *
   * Note that `Ready` and `Evicted` are derived from the artifact's independently synchronized
   * handle and are therefore best-effort snapshots, since it could be spilled/rebuilt at any time.
   */
  def status(key: HashBuildKey): BuildStatus = {
    val maybeFuture = synchronized { builds.get(key) }
    maybeFuture match {
      case None => BuildStatus.Cold
      case Some(future) if !future.isDone => BuildStatus.Building
      case Some(future) if future.isCompletedExceptionally => BuildStatus.Cold
      case Some(future) =>
        if (future.getNow(null).isReady) BuildStatus.Ready else BuildStatus.Evicted
    }
  }

  /** Return build statistics for a `key` only if it has a resident artifact. */
  def readyStats(key: HashBuildKey): Option[JoinBuildSideStats] = {
    val maybeFuture = synchronized { builds.get(key) }
    maybeFuture.flatMap { future =>
      if (future.isDone && !future.isCompletedExceptionally) {
        val artifact = future.getNow(null)
        if (artifact.isReady) Some(artifact.stats) else None
      } else {
        None
      }
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

    val artifact = try {
      future.get()
    } catch {
      case e: ExecutionException => throw e.getCause
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
      request: BackendJoinRequest): HashProbeBackend
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
      request: BackendJoinRequest): HashProbeBackend = {
    val side = request.requiredBuildSide.getOrElse(
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
   * - if the request has a required build side, that side takes precedence
   * - otherwise if build side selection is AUTO, record demand and defer to [[HashBuildPlanner]]
   * Selecting the offered side lazily constructs or acquires its cached artifact; selecting
   * the other side returns an on-demand backend.
   */
  override def backend(
      probe: HashProbeContext,
      request: BackendJoinRequest): HashProbeBackend = {
    // If the request has a required build side, bypass policy selection.
    val selectedSide = request.requiredBuildSide.getOrElse {
      val probeRows = if (offeredSide == GpuBuildLeft) {
        probe.rightRowCount
      } else {
        probe.leftRowCount
      }
      val (currentStatus, demand) = cache.synchronized {
        // Record the probe demand if the build cache is cold or evicted. Otherwise, get the
        // current demand for this hash build. Do this under the cache lock so that another
        // thread cannot concurrently change artifact status and reset demand.
        val observedStatus = cache.status(key)
        val recordDemand = probe.selection == JoinBuildSideSelection.AUTO &&
          (observedStatus == BuildStatus.Cold || observedStatus == BuildStatus.Evicted)
        val demand = if (recordDemand) {
          cache.observeProbe(key, demandId, probeRows)
        } else {
          cache.demand(key, demandId)
        }
        (observedStatus, demand)
      }
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
    // The callback resets demand if acquiring the artifact would reconstruct its native resource.
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
  // cuDF hash tables uses slots containing (hash, row-index) at a 0.5 load factor, resulting in
  // at least two slots per input row. This is a lower bound on the table size.
  private val MinHashTableBytesPerRow = 2L * (Integer.BYTES + Integer.BYTES)

  /** Estimated hash table size for spill accounting. */
  private[execution] def estimateHashTableSizeBytes(numRows: Long): Long =
    numRows * MinHashTableBytesPerRow

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

  /** Build the initial native artifact and capture the inputs needed to rebuild it. */
  private[execution] def create(
      buildBatch: SpillableColumnarBatch,
      boundKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean,
      prepareBatch: Option[ColumnarBatch => ColumnarBatch]): HashArtifact = {
    withBuildKeys(buildBatch, boundKeys, filterOutNulls, prepareBatch) { keys =>
      val stats = JoinBuildSideStats.fromTable(keys)
      val approxSizeInBytes = estimateHashTableSizeBytes(keys.getRowCount)
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
