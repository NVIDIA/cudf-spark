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

package com.nvidia.spark.rapids

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

/**
 * Bounded wait-order model of executor threads for broadcast eviction (issue #16460).
 *
 * Resources:
 *  - `Umm`: the Spark `UnifiedMemoryManager` monitor. `MemoryStore.evictBlocksToFreeSpace`
 *    serializes evicted blocks while it is held.
 *  - `Bcast`: the `SerializeConcatHostBuffersDeserializeBatch` monitor, held by `batch` and
 *    `doWriteObject`.
 *  - `Materialize`: the lock that `batch` holds while building the GPU batch after the fix.
 *  - GPU semaphore permits (`spark.rapids.sql.concurrentGpuTasks`).
 *  - GPU memory units. An allocation that does not fit waits in the RMM retry state until
 *    another thread frees memory.
 *
 * Each thread runs a fixed straight-line program. An operation on an unavailable resource
 * blocks the thread; there are no timeouts. Monitors are non-reentrant in the model because
 * no program re-enters one.
 *
 * Assumptions, stated separately from the checked property:
 *  - Device memory held by a GPU task in the model is not spillable (for example it backs a
 *    batch the task is actively using), so RMM can only satisfy a waiter when the holder frees.
 *  - RMM's deadlock breaker is not modelled. The model assumes it does not wake a thread waiting
 *    for device memory while other task threads are blocked on a JVM monitor or the GPU
 *    semaphore. This is an unverified hypothesis about the real breaker.
 *  - Waiting for a spill bounce buffer is not modelled. Bounce buffer holders are spill copies
 *    that do not take the memory manager monitor.
 *  - Thread programs are acyclic, so every infinite path is stuttering at a terminal state.
 *
 * Checked property: `AG EF allDone`. Self-loops are added only to states where every thread
 * finished. A non-finished state without successors is a deadlock. `AG EF allDone` detects it
 * under either totalization, but a deadlock-freedom check written as `AG EX true` does not if
 * every terminal state receives a self-loop.
 */
object ExecutorWaitOrderModel {
  sealed trait Monitor
  case object Umm extends Monitor
  case object Bcast extends Monitor
  case object Materialize extends Monitor

  sealed trait Op
  case class Lock(m: Monitor) extends Op
  case class Unlock(m: Monitor) extends Op
  case object AcquirePermit extends Op
  case object ReleasePermit extends Op
  case class Alloc(units: Int) extends Op
  case class Free(units: Int) extends Op

  case class Program(name: String, ops: Vector[Op])

  sealed trait EvictionVariant
  /** Current code: `doWriteObject` takes `Bcast`, then the GPU semaphore. */
  case object Current extends EvictionVariant
  /** Drop the semaphore but still materialize a spilled batch on the device under `Bcast`. */
  case object NaiveNoSemaphore extends EvictionVariant
  /** Drop the semaphore for a device-resident batch: no allocation, but still waits on `Bcast`. */
  case object NaiveResident extends EvictionVariant
  /**
   * The fix: `doWriteObject` snapshots state under `Bcast` and writes host bytes without the
   * semaphore or a device allocation, and `batch` allocates under `Materialize`, taking `Bcast`
   * only to publish the result.
   */
  case object Fixed extends EvictionVariant

  /** T1: a cache-store task without a permit whose storage request evicts a GPU broadcast. */
  def evictor(variant: EvictionVariant): Program = {
    val serialize = variant match {
      case Current => Vector(Lock(Bcast), AcquirePermit, Unlock(Bcast))
      case NaiveNoSemaphore => Vector(Lock(Bcast), Alloc(1), Free(1), Unlock(Bcast))
      case NaiveResident | Fixed => Vector(Lock(Bcast), Unlock(Bcast))
    }
    val tail = if (variant == Current) Vector(ReleasePermit) else Vector.empty
    Program("evictor", (Lock(Umm) +: serialize) ++ (Unlock(Umm) +: tail))
  }

  /**
   * T2: a GPU task that resolves the broadcast batch with a device allocation, then needs the
   * memory manager, for example to cache a broadcast block read back from disk.
   */
  def broadcastUser(variant: EvictionVariant): Program = {
    val materialize = variant match {
      case Fixed => Vector(Lock(Materialize), Alloc(1), Lock(Bcast), Unlock(Bcast),
        Unlock(Materialize))
      case _ => Vector(Lock(Bcast), Alloc(1), Unlock(Bcast))
    }
    Program("broadcastUser",
      (AcquirePermit +: materialize) ++ Vector(Lock(Umm), Unlock(Umm), Free(1), ReleasePermit))
  }

  /** T3: a GPU task holding device memory that needs the memory manager before freeing it. */
  val memoryHolder: Program = Program("memoryHolder", Vector(
    AcquirePermit, Alloc(1), Lock(Umm), Unlock(Umm), Free(1), ReleasePermit))

  /** The executor heartbeat reads memory metrics under the memory manager monitor. */
  val heartbeat: Program = Program("heartbeat", Vector(Lock(Umm), Unlock(Umm)))

  case class Config(programs: Vector[Program], permits: Int, gpuMemory: Int)

  case class State(
      pcs: Vector[Int],
      monitors: Map[Monitor, Int],
      permitHolders: Set[Int],
      memory: Vector[Int])

  case class Model(config: Config) {
    import config._

    val initial: State =
      State(Vector.fill(programs.size)(0), Map.empty, Set.empty, Vector.fill(programs.size)(0))

    def done(s: State): Boolean = programs.indices.forall(i => s.pcs(i) == programs(i).ops.size)

    private def usedMemory(s: State): Int = s.memory.sum

    def step(s: State, t: Int): Option[State] = {
      val pc = s.pcs(t)
      if (pc == programs(t).ops.size) {
        None
      } else {
        val advanced = s.copy(pcs = s.pcs.updated(t, pc + 1))
        programs(t).ops(pc) match {
          case Lock(m) if !s.monitors.contains(m) =>
            Some(advanced.copy(monitors = s.monitors + (m -> t)))
          case Unlock(m) =>
            require(s.monitors.get(m).contains(t), s"${programs(t).name} unlocks $m it lacks")
            Some(advanced.copy(monitors = s.monitors - m))
          case AcquirePermit if s.permitHolders.size < permits =>
            Some(advanced.copy(permitHolders = s.permitHolders + t))
          case ReleasePermit =>
            Some(advanced.copy(permitHolders = s.permitHolders - t))
          case Alloc(n) if usedMemory(s) + n <= gpuMemory =>
            Some(advanced.copy(memory = s.memory.updated(t, s.memory(t) + n)))
          case Free(n) =>
            Some(advanced.copy(memory = s.memory.updated(t, s.memory(t) - n)))
          case _ => None
        }
      }
    }

    case class Node(state: State, trace: Vector[String])

    /** Breadth-first exploration retaining the shortest trace to each reachable state. */
    lazy val reachable: Vector[Node] = {
      val queue = mutable.Queue(Node(initial, Vector.empty))
      val seen = mutable.HashSet(initial)
      val result = mutable.ArrayBuffer.empty[Node]
      while (queue.nonEmpty) {
        val node = queue.dequeue()
        result += node
        programs.indices.foreach { t =>
          step(node.state, t).foreach { succ =>
            if (seen.add(succ)) {
              val op = programs(t).ops(node.state.pcs(t))
              queue.enqueue(Node(succ, node.trace :+ s"${programs(t).name}:$op"))
            }
          }
        }
      }
      result.toVector
    }

    lazy val ids: Map[State, Int] = reachable.map(_.state).zipWithIndex.toMap

    /** Transition relation; self-loops only on finished states unless `totalizeAll`. */
    def edges(totalizeAll: Boolean = false): Set[(Int, Int)] = {
      val moves = reachable.flatMap { node =>
        programs.indices.flatMap(t => step(node.state, t)).map(ids(node.state) -> ids(_))
      }.toSet
      val sources = moves.map(_._1)
      val loops = reachable.map(n => ids(n.state)).filter { id =>
        !sources.contains(id) && (totalizeAll || done(reachable(id).state))
      }.map(id => id -> id)
      moves ++ loops
    }

    /** Non-finished reachable states without any enabled thread. */
    lazy val deadlocks: Vector[Node] = reachable.filter { node =>
      !done(node.state) && programs.indices.forall(t => step(node.state, t).isEmpty)
    }

    def describe(s: State): String = programs.indices.filterNot(t =>
      s.pcs(t) == programs(t).ops.size).map { t =>
      val held = s.monitors.collect { case (m, `t`) => m.toString }.toSeq ++
        (if (s.permitHolders.contains(t)) Seq("permit") else Seq.empty) ++
        (if (s.memory(t) > 0) Seq(s"mem=${s.memory(t)}") else Seq.empty)
      s"${programs(t).name} holds [${held.mkString(",")}] waits ${programs(t).ops(s.pcs(t))}"
    }.mkString("; ")
  }

  /** Set-based CTL evaluator over an explicit total transition relation. */
  case class Ctl(states: Set[Int], edges: Set[(Int, Int)]) {
    private val preds: Map[Int, Set[Int]] =
      edges.groupBy(_._2).map { case (t, es) => t -> es.map(_._1) }

    def ex(p: Set[Int]): Set[Int] = p.flatMap(t => preds.getOrElse(t, Set.empty))

    def eu(p: Set[Int], q: Set[Int]): Set[Int] = {
      var reached = q
      var next = reached ++ (p intersect ex(reached))
      while (next != reached) {
        reached = next
        next = reached ++ (p intersect ex(reached))
      }
      reached
    }

    def ef(p: Set[Int]): Set[Int] = eu(states, p)

    def ag(p: Set[Int]): Set[Int] = states -- ef(states -- p)
  }

  case class Result(
      holds: Boolean,
      agExTrue: Boolean,
      deadlocks: Vector[(Vector[String], String)],
      states: Int)

  def check(config: Config, totalizeAll: Boolean = false): Result = {
    val model = Model(config)
    val all = model.reachable.indices.toSet
    val ctl = Ctl(all, model.edges(totalizeAll))
    val doneStates = model.reachable.indices.filter(i => model.done(model.reachable(i).state))
    val agEfDone = ctl.ag(ctl.ef(doneStates.toSet))
    val init = model.ids(model.initial)
    Result(agEfDone.contains(init), ctl.ag(ctl.ex(all)).contains(init),
      model.deadlocks.map(n => n.trace -> model.describe(n.state)), all.size)
  }

  /** Reporter configuration: two GPU permits, one non-spillable unit of free device memory. */
  def reporterConfig(variant: EvictionVariant): Config =
    Config(Vector(evictor(variant), broadcastUser(variant), memoryHolder, heartbeat),
      permits = 2, gpuMemory = 1)

  /** Minimal configuration: one permit, the evictor and a permit holder needing the monitor. */
  def minimalConfig(variant: EvictionVariant): Config =
    Config(Vector(evictor(variant), Program("permitHolder",
      Vector(AcquirePermit, Lock(Umm), Unlock(Umm), ReleasePermit))), permits = 1, gpuMemory = 1)
}

class ExecutorWaitOrderModelSuite extends AnyFunSuite {
  import ExecutorWaitOrderModel._

  private def shortest(r: Result): (Vector[String], String) = r.deadlocks.minBy(_._1.size)

  private def assertDeadlock(config: Config, expected: String => Boolean): Unit = {
    val r = check(config)
    assert(!r.holds, "expected AG EF allDone to fail")
    assert(r.deadlocks.nonEmpty, "CTL failure must be explained by a reachable deadlock")
    val (trace, blocked) = shortest(r)
    info(s"${r.states} states; shortest deadlock: ${trace.mkString(" -> ")}")
    info(s"blocked: $blocked")
    assert(r.deadlocks.exists(d => expected(d._2)), r.deadlocks.map(_._2).mkString("\n"))
  }

  test("minimal: current doWriteObject waits on the permit under the memory manager") {
    assertDeadlock(minimalConfig(Current),
      b => b.contains("evictor holds") && b.contains("waits AcquirePermit"))
  }

  test("minimal: fixed eviction cannot deadlock") {
    val r = check(minimalConfig(Fixed))
    assert(r.holds && r.deadlocks.isEmpty)
  }

  test("reporter config: current doWriteObject deadlocks") {
    assertDeadlock(reporterConfig(Current), _.contains("waits AcquirePermit"))
  }

  test("reporter config: dropping only the semaphore still deadlocks") {
    assertDeadlock(reporterConfig(NaiveNoSemaphore),
      b => b.matches(".*evictor holds \\[[^]]*Umm.*"))
  }

  test("reporter config: fixed eviction satisfies AG EF allDone") {
    val r = check(reporterConfig(Fixed))
    info(s"${r.states} states")
    assert(r.holds && r.deadlocks.isEmpty, r.deadlocks.headOption.toString)
  }

  test("host-only eviction still deadlocks while batch allocates under the broadcast monitor") {
    assertDeadlock(reporterConfig(NaiveResident),
      b => b.contains("waits Lock(Bcast)") && b.contains("waits Alloc(1)"))
  }

  test("totalizing every terminal state hides the deadlock from AG EX true only") {
    val finishedOnly = check(reporterConfig(Current))
    val totalized = check(reporterConfig(Current), totalizeAll = true)
    assert(!finishedOnly.agExTrue && totalized.agExTrue)
    assert(!finishedOnly.holds && !totalized.holds)
  }

  test("CTL verdict agrees with explicit deadlock search for every variant") {
    Seq(Current, NaiveNoSemaphore, NaiveResident, Fixed).foreach { v =>
      Seq(minimalConfig(v), reporterConfig(v)).foreach { c =>
        val r = check(c)
        assert(r.holds == r.deadlocks.isEmpty && r.agExTrue == r.deadlocks.isEmpty, s"$v $c")
      }
    }
  }
}
