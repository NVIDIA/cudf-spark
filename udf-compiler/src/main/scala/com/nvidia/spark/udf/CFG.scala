/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import scala.annotation.tailrec
import scala.collection.immutable.{HashMap, SortedMap, SortedSet}

import CatalystExpressionBuilder.simplify
import javassist.bytecode.{CodeIterator, ConstPool, InstructionPrinter, Opcode}
import javassist.bytecode.analysis.Util

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

/**
 * Control Flow Graph (CFG)
 *
 * This file defines the basic blocks (BB), and a class that can generate a CFG
 * by leveraging [[LambdaReflection]].
 */

/**
 * A Basic Block (BB) is a set of instructions defined by [[instructionTable]],
 * where each entry in this table is a mapping of offset to [[Instruction]].
 *
 * The case class also provides some helpers, most importantly the [[propagateState]] helper
 * which generates a map of [[BB]] to [[State]].
 *
 * @param instructionTable
 */
case class BB(instructionTable: SortedMap[Int, Instruction]) extends Logging {
  def offset: Int = instructionTable.head._1

  def last: (Int, Instruction) = instructionTable.last

  def lastOffset: Int = last._1

  def lastInstruction: Instruction = last._2

  def propagateState(cfg: CFG, states: Map[BB, State]): Map[BB, State] = {
    val state@State(_, _, cond, expr) = states(this)
    logDebug(s"[BB.propagateState] propagating condition: ${cond} from state ${state} " +
        s"onto states: ${states}")
    lastInstruction.opcode match {
      case Opcode.IF_ICMPEQ | Opcode.IF_ICMPNE | Opcode.IF_ICMPLT |
           Opcode.IF_ICMPGE | Opcode.IF_ICMPGT | Opcode.IF_ICMPLE |
           Opcode.IFLT | Opcode.IFLE | Opcode.IFGT | Opcode.IFGE |
           Opcode.IFEQ | Opcode.IFNE | Opcode.IFNULL | Opcode.IFNONNULL => {
        logTrace(s"[BB.propagateState] lastInstruction: ${lastInstruction.instructionStr}")

        // An if statement has both a false and a true successor
        val (0, falseSucc) :: (1, trueSucc) :: Nil = cfg.successor(this)
        logTrace(s"[BB.propagateState] falseSucc ${falseSucc} trueSuccc ${trueSucc}")

        // cond is the entry condition into the condition block, and expr is the
        // actual condition for IF* (see Instruction.ifOp).
        // The entry conditions into the false branch are (cond && !expr) and
        // (cond && expr) respectively.
        //
        // For each successor, create a copy of the current state, and modify it
        // with the entry condition for the successor.  This ensures that the
        // state is propagated to the successors with the correct entry
        // conditions.
        //
        val falseState = state.copy(cond = simplify(And(cond, Not(expr.get))))
        val trueState = state.copy(cond = simplify(And(cond, expr.get)))

        logDebug(s"[BB.propagateState] States before: ${states}")

        // Each successor may already have the state populated if it has
        // multiple predecessors.
        // Update the states by merging the new state with the existing state.
        val newStates = (states
            + (falseSucc -> falseState.merge(states.get(falseSucc)))
            + (trueSucc -> trueState.merge(states.get(trueSucc))))
        logDebug(s"[BB.propagateState] States after: ${newStates}")
        newStates
      }
      case Opcode.TABLESWITCH | Opcode.LOOKUPSWITCH =>
        val table = cfg.successor(this).init
        val (-1, defaultSucc) = cfg.successor(this).last
        // Update the entry conditions of non-default successors based on the
        // match keys, and combine them to create the entry condition of the
        // default successor.
        val (defaultCondition, newStates) = (
            table.foldLeft[(Expression, Map[BB, State])]((Literal.TrueLiteral, states)) {
              case ((cond: Expression, currentStates: Map[BB, State]),
              (matchKey: Int, succ: BB)) =>
                val newState = state.copy(cond = simplify(EqualTo(expr.get, Literal(matchKey))))
                (And(Not(EqualTo(expr.get, Literal(matchKey))), cond),
                    currentStates + (succ -> newState.merge(currentStates.get(succ))))
            }
            )
        // Update the entry condition of the default successor.
        val defaultState = state.copy(cond = simplify(defaultCondition))
        newStates + (defaultSucc -> defaultState.merge(newStates.get(defaultSucc)))
      case Opcode.IRETURN | Opcode.LRETURN | Opcode.FRETURN | Opcode.DRETURN |
           Opcode.ARETURN | Opcode.RETURN | Opcode.ATHROW => states
      case _ =>
        val (0, successor) :: Nil = cfg.successor(this)
        // The condition, stack and locals from the current BB state need to be
        // propagated to its successor.
        states + (successor -> state.merge(states.get(successor)))
    }
  }
}

/**
 * The Control Flow Graph object.
 *
 * @param basicBlocks : the basic blocks for this CFG
 * @param predecessor : given a [[BB]] this maps the [[BB]]s to its predecessors
 * @param successor   : given a [[BB]] this maps the [[BB]]s to its successors.
 *                    Each element in the succssor list also has an Int value
 *                    which is used as a case for tableswitch and lookupswitch.
 */
case class CFG(basicBlocks: List[BB],
    predecessor: Map[BB, List[BB]],
    successor: Map[BB, List[(Int, BB)]])

/**
 * Companion object to generate a [[CFG]] instance given a [[LambdaReflection]]
 */
object CFG {
  /**
   * Iterate through the code to find out the basic blocks
   */
  def apply(lambdaReflection: LambdaReflection): CFG = {
    // find the last return in this lambda expression.
    // we use this in scala 2.13+ because we see new RETURN instructions where
    // scala 2.12 would before use GOTO. We are undoing this while we parse the
    // bytecode because we would like to not complicate our code parsing logic
    // to merge different branches (each RETURN would be a leaf). By turning this
    // into GOTO [last return] we bring back the old behavior of scala 2.12.
    val codeIterator = lambdaReflection.codeIterator
    codeIterator.begin()
    var lastReturnOffset = 0
    while(codeIterator.hasNext) {
      val offset = codeIterator.next()
      val opcode: Int = codeIterator.byteAt(offset)
      if (Util.isReturn(opcode)) {
        lastReturnOffset = offset
      }
    }

    // labels: targets of branching instructions (offset)
    // edges: connection between branch instruction offset, and target offsets (successors)
    //        if ifeq then there would be a true and a false successor
    //        if return there would be no successors (likely)
    //        goto has 1 successors
    codeIterator.begin()
    val (labels, edges) = collectLabelsAndEdges(
      codeIterator, lambdaReflection.constPool, lastReturnOffset)

    codeIterator.begin() // rewind
    val instructionTable = createInstructionTable(
      codeIterator, lambdaReflection.constPool, lastReturnOffset)

    val (basicBlocks, offsetToBB) = createBasicBlocks(labels, instructionTable)

    val (predecessor, successor) = connectBasicBlocks(basicBlocks, offsetToBB, edges)

    CFG(basicBlocks, predecessor, successor)
  }

  @tailrec
  private def collectLabelsAndEdges(codeIterator: CodeIterator,
      constPool: ConstPool,
      lastReturnOffset: Int,
      labels: SortedSet[Int] = SortedSet(),
      edges: SortedMap[Int, List[(Int, Int)]] = SortedMap())
  : (SortedSet[Int], SortedMap[Int, List[(Int, Int)]]) = {
    if (codeIterator.hasNext) {
      val offset: Int = codeIterator.next
      val nextOffset: Int = codeIterator.lookAhead
      val opcode: Int = codeIterator.byteAt(offset)
      // here we are looking for branching instructions
      opcode match {
        case _ if Util.isReturn(opcode) && offset != lastReturnOffset =>
          // if we had any return along the way, we are going to replace it
          // with a GOTO [lastReturnOffset]
          collectLabelsAndEdges(
            codeIterator, constPool, lastReturnOffset,
            labels + lastReturnOffset,
            edges + (offset -> List((0, lastReturnOffset))))
        case Opcode.IF_ICMPEQ | Opcode.IF_ICMPNE | Opcode.IF_ICMPLT |
             Opcode.IF_ICMPGE | Opcode.IF_ICMPGT | Opcode.IF_ICMPLE |
             Opcode.IFEQ | Opcode.IFNE | Opcode.IFLT | Opcode.IFGE |
             Opcode.IFGT | Opcode.IFLE | Opcode.IFNULL | Opcode.IFNONNULL =>
          // an if statement has two other offsets, false and true branches.

          // the false offset is the next offset, per the definition of if<cond>
          val falseOffset = nextOffset

          // in jvm, the if<cond> ops are followed by two bytes, which are to be
          // used together (s16bitAt does this for us) only for the success case of the if
          val trueOffset = offset + codeIterator.s16bitAt(offset + 1)

          // keep iterating, having added the false and true offsets to the labels,
          // and having added the edges (if offset -> List(false offset, true offset))
          collectLabelsAndEdges(
            codeIterator, constPool, lastReturnOffset,
            labels + falseOffset + trueOffset,
            edges + (offset -> List((0, falseOffset), (1, trueOffset))))
        case Opcode.TABLESWITCH =>
          val defaultOffset = (offset + 4) / 4 * 4
          val default = (-1, offset + codeIterator.s32bitAt(defaultOffset))
          val lowOffset = defaultOffset + 4
          val low = codeIterator.s32bitAt(lowOffset)
          val highOffset = lowOffset + 4
          val high = codeIterator.s32bitAt(highOffset)
          val tableOffset = highOffset + 4
          val table = List.tabulate(high - low + 1) { i =>
            (low + i, offset + codeIterator.s32bitAt(tableOffset + i * 4))
          } :+ default
          collectLabelsAndEdges(
            codeIterator, constPool, lastReturnOffset,
            labels ++ table.map(_._2),
            edges + (offset -> table))
        case Opcode.LOOKUPSWITCH =>
          val defaultOffset = (offset + 4) / 4 * 4
          val default = (-1, offset + codeIterator.s32bitAt(defaultOffset))
          val npairsOffset = defaultOffset + 4
          val npairs = codeIterator.s32bitAt(npairsOffset)
          val tableOffset = npairsOffset + 4
          val table = List.tabulate(npairs) { i =>
            (codeIterator.s32bitAt(tableOffset + i * 8),
              offset + codeIterator.s32bitAt(tableOffset + i * 8 + 4))
          } :+ default
          collectLabelsAndEdges(
            codeIterator, constPool, lastReturnOffset,
            labels ++ table.map(_._2),
            edges + (offset -> table))
        case Opcode.GOTO | Opcode.GOTO_W =>
          // goto statements have a single address target, we must go there
          val getOffset = if (opcode == Opcode.GOTO) {
            codeIterator.s16bitAt(_)
          } else {
            codeIterator.s32bitAt(_)
          }
          val labelOffset = offset + getOffset(offset + 1)
          collectLabelsAndEdges(
            codeIterator, constPool, lastReturnOffset,
            labels + labelOffset,
            edges + (offset -> List((0, labelOffset))))
        case Opcode.IF_ACMPEQ | Opcode.IF_ACMPNE |
             Opcode.JSR | Opcode.JSR_W | Opcode.RET =>
          val instructionStr = InstructionPrinter.instructionString(
            codeIterator, offset, constPool)
          throw new SparkException("Unsupported instruction: " + instructionStr)
        case _ => collectLabelsAndEdges(codeIterator, constPool, lastReturnOffset,
          labels, edges)
      }
    } else {
      // base case
      (labels, edges)
    }
  }

  @tailrec
  private def createInstructionTable(
      codeIterator: CodeIterator,
      constPool: ConstPool,
      lastReturnOffset: Int,
      instructionTable: SortedMap[Int, Instruction] = SortedMap())
  : SortedMap[Int, Instruction] = {
    if (codeIterator.hasNext) {
      val offset = codeIterator.next
      val opcode = codeIterator.byteAt(offset)
      if (Util.isReturn(opcode) && offset != lastReturnOffset) {
        // an internal RETURN is replaced by GOTO to the last return of the
        // lambda.
        val instruction = Instruction(Opcode.GOTO, lastReturnOffset, "GOTO")
        createInstructionTable(codeIterator, constPool, lastReturnOffset,
          instructionTable + (offset -> instruction))
      } else {
        val instructionStr = InstructionPrinter.instructionString(codeIterator, offset, constPool)
        val instruction = Instruction(codeIterator, offset, instructionStr)
        createInstructionTable(codeIterator, constPool, lastReturnOffset,
          instructionTable + (offset -> instruction))
      }
    } else {
      instructionTable
    }
  }

  @tailrec
  private def createBasicBlocks(labels: SortedSet[Int],
      instructionTable: SortedMap[Int, Instruction],
      basicBlocks: List[BB] = List(),
      offsetToBB: Map[Int, BB] = HashMap()): (List[BB], Map[Int, BB]) = {
    if (labels.isEmpty) {
      val instructions = instructionTable
      val bb = BB(instructions)
      ((bb +: basicBlocks).reverse,
          instructions.foldLeft(offsetToBB) { case (offsetToBB, (offset, _)) =>
            offsetToBB + (offset -> bb)
          })
    } else {
      // get instuctions prior to the first label (branch) we are looking at
      val (instructions, instructionsForOtherBBs) = instructionTable.span(_._1 < labels.head)

      // BB is a node in the CFG, BB -> BB connects via branch
      val bb = BB(instructions) // put the instructions that belong together into a BB

      // create more BB's with the rest of the instructions post branch
      createBasicBlocks(
        labels.tail,
        instructionsForOtherBBs,
        // With immutable linked list, prepend is faster than append.
        // basicBlocks is an immutable linked list.
        bb +: basicBlocks,
        instructions.foldLeft(offsetToBB) { case (offsetToBB, (offset, _)) =>
          offsetToBB + (offset -> bb)
        })
    }
  }

  @tailrec
  private def connectBasicBlocks(basicBlocks: List[BB],
      offsetToBB: Map[Int, BB],
      edges: SortedMap[Int, List[(Int, Int)]],
      predecessor: Map[BB, List[BB]] = Map().withDefaultValue(Nil),
      successor: Map[BB, List[(Int, BB)]] = Map().withDefaultValue(Nil))
  : (Map[BB, List[BB]], Map[BB, List[(Int, BB)]]) = {
    if (basicBlocks.isEmpty) {
      (predecessor, successor)
    } else {
      // Connect the first basic block in basicBlocks (src) with its predecssors
      // and successors.
      val src :: rest = basicBlocks
      // Get the destination basic blocks (dst) of the edges that connect from
      // src.
      val dst = edges.getOrElse(src.lastOffset,
        if (rest.isEmpty) {
          List()
        } else {
          List((0, rest.head.offset))
        }).map { case (k, v) => (k, offsetToBB(v)) }
      // Recursively call connectBasicBlocks with the rest of basicBlocks.
      connectBasicBlocks(
        rest,
        offsetToBB,
        edges,
        //For each basic block, l, in dst, update predecessor map by prepending
        //src to predecessor(l).
        dst.foldLeft(predecessor) { case (p: Map[BB, List[BB]], (_, l)) => {
          p + (l -> (src :: p(l)))
        }
        },
        // Add src -> dst to successor map.
        successor + (src -> dst))
    }
  }
}
