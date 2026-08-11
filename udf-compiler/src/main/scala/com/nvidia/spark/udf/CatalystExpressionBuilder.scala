/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import javassist.CtClass

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * CatalystExpressionBuilder
 *
 * This compiles a scala lambda expression into a catalyst expression.
 *
 * Here are the high-level steps:
 *
 * 1) Use SerializedLambda and javaassist to get a reflection based interface to `function`
 * this is done in [[LambdaReflection]]
 *
 * 2) Obtain the Control Flow Graph (CFG) using the reflection interface obtained above.
 *
 * 3) Get catalyst `Expressions` based on the basic blocks [[BB]] obtained in the CFG
 * and simplify before replacing in the Spark Logical Plan.
 *
 * @param function the original Scala UDF provided by the user
 */
case class CatalystExpressionBuilder(private val function: AnyRef) extends Logging {
  final private val lambdaReflection: LambdaReflection = LambdaReflection(function)

  final private val cfg = CFG(lambdaReflection)

  /**
   * [[compile]]: Entry point for [[CatalystExpressionBuilder]].
   *
   * With this function we:
   *
   * 1) Create a starting [[State]], which ultimately is used to keep track of
   * the locals, stack, condition, expression.
   *
   * 2) Pick out the head Basic Block (BB) from the Control Flow Graph (CFG)
   * NOTE: that this picks the head element, and then recurses
   *
   * 3) Feed head BB to the start node
   *
   * @param children a sequence of catalyst arguments to the udf.
   * @return the compiled expression, optionally
   */
  def compile(children: Seq[Expression], objref: Option[Expression] = None): Option[Expression] = {

    // create starting state, this will be:
    //   State([children expressions], [empty stack], cond = true, expr = None)
    val entryState = State.makeStartingState(lambdaReflection, children, objref)

    // pick first of the Basic Blocks, and start recursing
    val entryBlock = cfg.basicBlocks.head

    logDebug(s"[CatalystExpressionBuilder] Attempting to compile: ${function}, " +
        s"with children: ${children}, " + s"entry block: ${entryBlock}, and " +
        s"entry state: ${entryState}")

    // start recursing
    val compiled = doCompile(List(entryBlock), Map(entryBlock -> entryState)).map { e =>
      if (lambdaReflection.ret == CtClass.booleanType) {
        // JVM bytecode returns an integer value when the return type is
        // boolean, hence the cast.
        CatalystExpressionBuilder.simplify(Cast(e, BooleanType))
      } else {
        e
      }
    }

    if (compiled.isEmpty) {
      logDebug(s"[CatalystExpressionBuilder] failed to compile")
    } else {
      val expr = compiled.get
      val internal = expr.find(_.isInstanceOf[Repr.CompilerInternal])
      internal.foreach { e =>
        throw new IllegalStateException(
          s"compiled UDF has compiler internal expression $e: $expr")
      }
      logDebug(s"[CatalystExpressionBuilder] compiled expression: $expr")
    }

    compiled
  }

  /**
   * doCompile: using a starting basic block and state, produce a new [[State]] based on the
   * basic block's [[Instruction]] table, and ultimately recurse through the successor(s)
   * of the basic block we are currently visiting in the [[CFG]], using Depth-First Traversal.
   *
   * 1) [[compile]] calls [[doCompile]] with the head [[BB]] of [[cfg]] and its [[State]].
   *
   * 2) [[doCompile]] will fold the currently visiting [[BB]]'s instruction table,
   * by making a new [[State]], that consumes the prior [[State]]
   *
   * 3) With the new states, we call the visiting block's [[BB.propagateState]],
   * and propagate its state to update successors' states.
   *
   * 4) If the block's last instruction is a return, this part of the graph has reached its end,
   * return whatever expression was accrued in [[State.expr]]
   *
   * else, recurse
   *
   * @param worklist stack for depth-first traversal.
   * @param states   map between [[BB]]s and their [[State]]s.  Each [[State]]
   *                 keeps track of locals, evaluation stack, and condition for the [[BB]].
   * @param pending  [[BB]]s that are ready to be added to worklist
   *                 once all their predecessors have been visited (have a count of 0).
   * @param visited  the set of [[BB]] we have seen so far.  It is used to make
   *                 sure each [[BB]] is visited only once.
   * @return the compiled expression, optionally
   */
  @tailrec
  private def doCompile(worklist: List[BB],
      states: Map[BB, State],
      pending: Map[BB, Int] = cfg.predecessor.mapValues(_.size).toMap,
      visited: Set[BB] = Set()): Option[Expression] = {
    /**
     * Pick the first block, and store the rest of the list in [[rest]].
     *
     * 1) Initially, [[worklist] is [[CFG.head]] :: nil
     * 2) As we recurse, [[worklist]] gets new [[BB]]s when the all of its predecessors are
     * visited.
     * 3) The head [[BB]] ([[basicBlock]]), then goes through the compilation process:
     * i) [[State]] is obtained (at the beginning, there's a seed [[State]] added in [[compile]]
     * ii) after each iteration, new [[State]] is created for [[basicBlock]]. This is the
     * first step where we take
     * javaassist Opcode foreach [[Instruction]] in the [[BB]]'s instruction table, and turn
     * it into [[State]]
     * objects with: locals, stack, condition, and an evolving catalyst expression.
     * ii) the state is then propagated:
     *
     */

    val basicBlock :: rest = worklist

    // find the state associated with this BB
    val state: State = states(basicBlock)

    logTrace(s"States for basic block ${basicBlock} => ${state}")

    /**
     * Iterate through the instruction table for the BB:
     * Using [[state]] as the starting value, apply the instruction [[Instruction.apply]]
     * to obtain a new [[State]]. This new state is passed back to [[Instruction.apply]],
     * as foldLeft makes its way through the BB's Instruction Table.
     */
    val it: Map[Int, Instruction] = basicBlock.instructionTable

    val newState: State = it.foldLeft(state) { (st: State, i: (Int, Instruction)) =>
      val instruction: Instruction = i._2
      instruction.makeState(lambdaReflection, basicBlock, st)
    }

    val sb = new StringBuilder()
    sb.append(s"[CatalystExpressionBuilder.doCompile] Basic Block ${basicBlock}")

    // when you have branching expressions, we need to look at both the true and false expressions
    // if (x > 0) 1 else 0
    val newStates = basicBlock.propagateState(cfg, states + (basicBlock -> newState))

    // This is testing whether the last instruction of the basic block is a return.
    // A basic block can have other branching instructions as the last instruction,
    // otherwise.
    if (basicBlock.lastInstruction.isReturn) {
      // When the compiled method throws exceptions, we combine the returned
      // expression (expr) with exceptions (except0, except1, except2.., exceptn)
      // based on exception conditions (cond0, cond1, cond2, .., condn) as follows:
      //         If
      //         |
      //  +------+------+
      //  |      |      |
      // cond0 except0  If
      //                |
      //         +------+------+
      //         |      |      |
      //        cond1 except1  If
      //                       |
      //                +------+------+
      //                |      |      |
      //               cond2 except2  If
      //                              |
      //                              .
      //                              .
      //                              .
      //                       +------+------+
      //                       |      |      |
      //                      condn exceptn  expr
      newStates.foldRight(newStates(basicBlock).expr) { case ((bb, s), expr) =>
        if (bb.lastInstruction.isThrow) {
          Some(If(s.cond, s.stack.head, expr.get))
        } else {
          expr
        }
      }
    } else {
      // account for this block in visited
      val newVisited = visited + basicBlock

      /**
       * 1) For the currently vising [[BB]], get the successors from the [[CFG]].
       * 2) The foldLeft this list, into a list of successors ([[readySucc]]), and
       * a map of predecessor [[newPending]] counts
       *
       * Among the successors of the current [[BB]], find the ones that are
       * ready for traversal.  They are added to [[readySucc]] and removed from
       * [[pending]] to create [[newPending]].
       *
       * A succesor is ready for traversal, if all of its predecessors have been visited.
       */
      val (readySucc: List[BB], newPending: Map[BB, Int]) =
        cfg.successor(basicBlock).foldLeft((List[BB](), pending)) {
          case (x@(remaining: List[BB], currentPending: Map[BB, Int]), (_, successor)) =>
            if (newVisited(successor)) {
              // This successor has already been visited through another path.
              // Do not update readySucc or newPending.
              x
            } else {
              // [[currentPending]] is used to make sure that a [[BB]] is visited after
              // all its predecessors have been visited.
              val count = currentPending(successor) - 1
              if (count > 0) {
                // overwrite decremented successor's pending count
                (remaining, // readySucc
                    currentPending + (successor -> count)) // newPending
              } else {
                // count <= 0
                // add successor to the remaining, remove from pending.
                (successor :: remaining, // readySucc
                    currentPending - successor) // newPending
              }
            }
        }

      if (rest.isEmpty && readySucc.isEmpty && newPending.nonEmpty) {
        // We allow a node to be visited only after all its predecessors
        // are visited, but if a node is the entry to a loop, all its
        // predecessors cannot be visited unless this node is visited.
        // This case results in an empty worklist with non-empty new pending
        // list.
        throw new SparkException("Unsupported control flow: loop")
      }

      doCompile(
        readySucc ::: rest,
        newStates,
        newPending,
        newVisited)
    }
  }
}

/**
 * CatalystExpressionBuilder helper object, contains a function that is used to
 * simplify a directly translated catalyst expression (from bytecode) into something simpler
 * that the remaining catalyst optimizations can handle.
 */
object CatalystExpressionBuilder extends Logging {
  /** simplify: given a raw converted catalyst expression, attempt to match patterns to simplify
   * before handing it over to catalyst optimizers (the LogicalPlan does this later).
   *
   * It is called from [[State.merge]], from itself, and from [[BB.propagateState]].
   *
   * @param expr
   * @return
   */
  @tailrec
  final def simplify(expr: Expression): Expression = {
    def simplifyExpr(expr: Expression): Expression = {
      val res = expr match {
        case And(Literal.TrueLiteral, c) => simplifyExpr(c)
        case And(c, Literal.TrueLiteral) => simplifyExpr(c)
        case and: And if and.containsChild(Literal.FalseLiteral) => Literal.FalseLiteral
        case And(c1@LessThan(s1, Literal(v1, t1)),
        c2@LessThan(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] < v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] < v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1@LessThanOrEqual(s1, Literal(v1, t1)),
        c2@LessThanOrEqual(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] < v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] < v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1@LessThanOrEqual(s1, Literal(v1, t1)),
        c2@LessThan(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] < v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] < v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1@GreaterThan(s1, Literal(v1, t1)),
        c2@GreaterThan(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] > v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] > v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1@GreaterThanOrEqual(s1, Literal(v1, t1)),
        c2@GreaterThanOrEqual(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] > v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] > v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1@GreaterThan(s1, Literal(v1, t1)),
        c2@GreaterThanOrEqual(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] >= v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] >= v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case And(c1, c2) => And(simplifyExpr(c1), simplifyExpr(c2))
        case or: Or if or.containsChild(Literal.TrueLiteral) => Literal.TrueLiteral
        case Or(Literal.FalseLiteral, c) => simplifyExpr(c)
        case Or(c, Literal.FalseLiteral) => simplifyExpr(c)
        case Or(c1@GreaterThan(s1, Literal(v1, t1)),
        c2@GreaterThanOrEqual(s2, Literal(v2, t2))) if s1 == s2 && t1 == t2 => {
          t1 match {
            case IntegerType =>
              if (v1.asInstanceOf[Int] < v2.asInstanceOf[Int]) {
                c1
              } else {
                c2
              }
            case LongType =>
              if (v1.asInstanceOf[Long] < v2.asInstanceOf[Long]) {
                c1
              } else {
                c2
              }
            case _ => expr
          }
        }
        case Or(c1, c2) => Or(simplifyExpr(c1), simplifyExpr(c2))
        case Not(Literal.TrueLiteral) => Literal.FalseLiteral
        case Not(Literal.FalseLiteral) => Literal.TrueLiteral
        case Not(LessThan(c1, c2)) => GreaterThanOrEqual(c1, c2)
        case Not(LessThanOrEqual(c1, c2)) => GreaterThan(c1, c2)
        case Not(GreaterThan(c1, c2)) => LessThanOrEqual(c1, c2)
        case Not(GreaterThanOrEqual(c1, c2)) => LessThan(c1, c2)
        case Not(c) => Not(simplifyExpr(c))
        case EqualTo(Literal(v1, _), Literal(v2, _)) =>
          if (v1 == v2) Literal.TrueLiteral else Literal.FalseLiteral
        case LessThan(If(c1,
        Literal(1, _),
        If(c2,
        Literal(-1, _),
        Literal(0, _))),
        Literal(0, _)) => simplifyExpr(And(Not(c1), c2))
        case LessThanOrEqual(If(c1,
        Literal(1, _),
        If(_,
        Literal(-1, _),
        Literal(0, _))),
        Literal(0, _)) => simplifyExpr(Not(c1))
        case GreaterThan(If(c1,
        Literal(1, _),
        If(_,
        Literal(-1, _),
        Literal(0, _))),
        Literal(0, _)) => c1
        case GreaterThanOrEqual(If(c1,
        Literal(1, _),
        If(c2,
        Literal(-1, _),
        Literal(0, _))),
        Literal(0, _)) => simplifyExpr(Or(c1, Not(c2)))
        case EqualTo(If(c1,
        Literal(1, _),
        If(c2,
        Literal(-1, _),
        Literal(0, _))),
        Literal(0, _)) => simplifyExpr(And(Not(c1), Not(c2)))
        case If(_, t, f) if t == f => t
        // JVMachine encodes boolean array components using 1 to represent true
        // and 0 to represent false (see
        // https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.4).
        case ce: Cast if ce.child.isInstanceOf[Literal] && ce.dataType == BooleanType =>
          ce.child match {
            case Literal(0, _) => Literal.FalseLiteral
            case Literal(1, _) => Literal.TrueLiteral
          }
        case ce: Cast if ce.child.isInstanceOf[If] && ce.dataType == BooleanType =>
          ce.child match {
            case If(c, t, f) =>
          simplifyExpr(If(simplifyExpr(c),
            simplifyExpr(Cast(t, BooleanType, ce.timeZoneId)),
            simplifyExpr(Cast(f, BooleanType, ce.timeZoneId))))
          }
        case If(c, Repr.ArrayBuffer(t), Repr.ArrayBuffer(f)) => Repr.ArrayBuffer(If(c, t, f))
        case If(c, Repr.StringBuilder(t), Repr.StringBuilder(f)) => Repr.StringBuilder(If(c, t, f))
        case If(c, Literal.TrueLiteral, Literal.FalseLiteral) => c
        case If(c, Literal.FalseLiteral, Literal.TrueLiteral) => Not(c)
        case If(c, t, f) => If(simplifyExpr(c), simplifyExpr(t), simplifyExpr(f))
        case _ => expr
      }
      logDebug(s"[CatalystExpressionBuilder] simplify: ${expr} ==> ${res}")
      res
    }

    val simplifiedExpr = simplifyExpr(expr)
    if (simplifiedExpr == expr) simplifiedExpr else simplify(simplifiedExpr)
  }
}
