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

package com.nvidia.spark.rapids.spill

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

private[spill] object SpillLifecycleModel {
  sealed trait Phase
  case object Ready extends Phase
  case object Copying extends Phase
  case object Copied extends Phase
  case object Finished extends Phase

  sealed trait Action
  case object Acquire extends Action
  case object Release extends Action
  case object QueueGpuRead extends Action
  case object CompleteGpuRead extends Action
  case object StartSpill extends Action
  case object CompleteCopy extends Action
  case object FinishSpill extends Action
  case object Close extends Action

  val actions: Seq[Action] = Seq(Acquire, Release, QueueGpuRead, CompleteGpuRead,
    StartSpill, CompleteCopy, FinishSpill, Close)

  case class State(
      phase: Phase = Ready,
      closed: Boolean = false,
      borrowed: Boolean = false,
      acquired: Boolean = false,
      gpuReadPending: Boolean = false,
      gpuReadQueued: Boolean = false,
      deviceOwned: Boolean = true,
      hostOwned: Boolean = false,
      deviceReleases: Int = 0)

  case class Node(state: State, trace: Vector[Action])

  def next(state: State, action: Action): Option[State] = action match {
    case Acquire if !state.closed && !state.borrowed && !state.acquired &&
        state.phase != Finished =>
      Some(state.copy(borrowed = true, acquired = true))
    case Release if state.borrowed =>
      Some(state.copy(borrowed = false))
    case QueueGpuRead if state.borrowed && !state.closed && !state.gpuReadQueued &&
        state.phase != Finished =>
      Some(state.copy(borrowed = false, gpuReadPending = true, gpuReadQueued = true))
    case CompleteGpuRead if state.gpuReadPending =>
      Some(state.copy(gpuReadPending = false))
    case StartSpill if state.phase == Ready && !state.closed && !state.borrowed =>
      Some(state.copy(phase = Copying))
    case CompleteCopy if state.phase == Copying =>
      Some(state.copy(phase = Copied, hostOwned = !state.closed))
    case FinishSpill if state.phase == Copied && !state.gpuReadPending =>
      Some(state.copy(phase = Finished, deviceOwned = false, deviceReleases = 1))
    case Close if !state.closed && (state.phase != Ready || !state.gpuReadPending) =>
      val releaseNow = state.phase == Ready || state.phase == Finished
      Some(state.copy(closed = true, hostOwned = false,
        deviceOwned = if (releaseNow) false else state.deviceOwned,
        deviceReleases = if (releaseNow && state.deviceOwned) 1 else state.deviceReleases))
    case _ => None
  }

  def violations(state: State): Seq[String] = {
    val failures = mutable.ArrayBuffer.empty[String]
    if (state.deviceReleases > 1) {
      failures += "device allocation released more than once"
    }
    if (state.gpuReadPending && !state.deviceOwned) {
      failures += "device allocation released before GPU reader completed"
    }
    if (!state.closed && state.phase == Finished && !state.hostOwned) {
      failures += "successful spill has no recoverable copy"
    }
    if (state.closed && state.phase == Finished &&
        (state.deviceOwned || state.hostOwned)) {
      failures += "closed handle retains a backing allocation"
    }
    failures.toSeq
  }

  def explore(): Vector[Node] = {
    val initial = Node(State(), Vector.empty)
    val queue = mutable.Queue(initial)
    val seen = mutable.HashSet(initial.state)
    val result = mutable.ArrayBuffer.empty[Node]
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      result += node
      actions.foreach { action =>
        next(node.state, action).foreach { successor =>
          if (seen.add(successor)) {
            queue.enqueue(Node(successor, node.trace :+ action))
          }
        }
      }
    }
    result.toVector
  }
}

class SpillLifecycleModelSuite extends AnyFunSuite {
  import SpillLifecycleModel._

  test("bounded device spill lifecycle satisfies safety invariants") {
    val states = explore()
    assert(states.nonEmpty)
    val failure = states.iterator.flatMap(node =>
      violations(node.state).map(_ -> node.trace)).take(1).toList.headOption
    assert(failure.isEmpty, failure.toString)
    assert(states.exists(_.state.phase == Finished))
    assert(states.exists(node => node.state.closed && node.state.phase == Copying))
    assert(states.exists(node => node.state.gpuReadPending && node.state.phase == Copied))
  }

  test("invalid release before GPU completion has a counterexample") {
    val state = explore().find(node =>
      node.state.gpuReadPending && node.state.phase == Copied).get
    val invalid = state.state.copy(deviceOwned = false, deviceReleases = 1)
    assert(violations(invalid).contains("device allocation released before GPU reader completed"))
  }
}
