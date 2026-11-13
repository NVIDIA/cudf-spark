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

/**
 * Packs AST JIT roots into multi-output groups. Roots that share an operation stay in one group
 * when the group fits the limits, so cuDF can evaluate the shared operation once.
 */
private[rapids] object GpuAstJitGrouping {
  final case class Limits(maxOps: Int, maxOutputs: Int)

  /** An output root and the operations its program evaluates. */
  final case class Root[K](index: Int, operations: Set[K])

  private final class Group[K](val id: Int) {
    val roots: mutable.ArrayBuffer[Root[K]] = mutable.ArrayBuffer.empty
    val operations: mutable.HashSet[K] = mutable.HashSet.empty

    def canAdd(newRoots: Seq[Root[K]], limits: Limits): Boolean = {
      val addedOps = newRoots.iterator.flatMap(_.operations).toSet.count(!operations.contains(_))
      roots.size <= limits.maxOutputs - newRoots.size &&
        operations.size <= limits.maxOps - addedOps
    }

    def add(newRoots: Seq[Root[K]]): Unit = {
      roots ++= newRoots
      newRoots.foreach(root => operations ++= root.operations)
    }
  }

  private def connectedComponents[K](roots: Seq[Root[K]]): Seq[Seq[Root[K]]] = {
    val parents = roots.indices.toArray

    def find(index: Int): Int = {
      var root = index
      while (parents(root) != root) {
        root = parents(root)
      }
      var current = index
      while (parents(current) != current) {
        val next = parents(current)
        parents(current) = root
        current = next
      }
      root
    }

    def union(left: Int, right: Int): Unit = {
      val leftRoot = find(left)
      val rightRoot = find(right)
      if (leftRoot != rightRoot) {
        parents(rightRoot) = leftRoot
      }
    }

    val operationOwner = mutable.HashMap.empty[K, Int]
    roots.zipWithIndex.foreach { case (root, index) =>
      root.operations.foreach { operation =>
        operationOwner.get(operation) match {
          case Some(previous) => union(previous, index)
          case None => operationOwner.put(operation, index)
        }
      }
    }

    val components = mutable.LinkedHashMap.empty[Int, mutable.ArrayBuffer[Root[K]]]
    roots.indices.foreach { index =>
      components.getOrElseUpdate(find(index), mutable.ArrayBuffer.empty) += roots(index)
    }
    components.values.map(_.toSeq).toSeq
  }

  /** Returns the group ID of each root, keyed by `Root.index`. */
  def assign[K](roots: Seq[Root[K]], limits: Limits): Map[Int, Int] = {
    val groups = mutable.ArrayBuffer.empty[Group[K]]

    def newGroup(): Group[K] = {
      val group = new Group[K](groups.size)
      groups += group
      group
    }

    connectedComponents(roots).foreach { component =>
      val componentOps = component.iterator.flatMap(_.operations).toSet
      if (component.size <= limits.maxOutputs && componentOps.size <= limits.maxOps) {
        val group = groups.find(_.canAdd(component, limits)).getOrElse(newGroup())
        group.add(component)
      } else {
        val componentGroups = mutable.ArrayBuffer.empty[Group[K]]
        component.foreach { root =>
          val candidates = componentGroups.filter(_.canAdd(Seq(root), limits))
          val group = if (candidates.nonEmpty) {
            candidates.maxBy(group => root.operations.count(group.operations.contains))
          } else {
            val created = newGroup()
            componentGroups += created
            created
          }
          group.add(Seq(root))
        }
      }
    }

    groups.iterator.flatMap { group =>
      group.roots.iterator.map(root => root.index -> group.id)
    }.toMap
  }
}
