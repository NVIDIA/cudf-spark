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

package com.nvidia.spark.rapids.perf

import java.time.Duration
import java.util.{Collections, List => JList}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{DimValue, MetricStore, Observation, SchemaStatus, Status,
  SummaryRequest, SummaryResponse}
import com.nvidia.spark.rapids.HistoryMetricsManager

import org.apache.spark.internal.Logging

/**
 * Time budgets for store calls.
 *
 * @param planningTimeout budget for one lookup on the planning path
 * @param declareTimeout budget for each family's one declaration per store
 */
case class HistoryPolicy(planningTimeout: Duration, declareTimeout: Duration)

/**
 * One metric family over the installed `MetricStore`: declare, record, read the latest value.
 * Never manages the provider.
 *
 * Every failure abstains (caller keeps its static decision) and is logged once per JVM per
 * reason.
 */
final class MetricHistory(val family: HistoryMetric, store: () => MetricStore) extends Logging {

  /** Last store declared in, and whether it accepted. */
  private case class Declaration(in: MetricStore, accepted: Boolean)

  @volatile private var declaration: Declaration = Declaration(null, accepted = false)

  private val reported = ConcurrentHashMap.newKeySet[String]()

  /** Declares once per store; recording requires a declaration in this process. */
  def declare(policy: HistoryPolicy): Boolean = declaredIn(store(), policy)

  /**
   * Latest value for `key` within the planning age, or None (untrackable, no data, error).
   *
   * @param key a value for each of the family's dimensions; evaluated here, so a value the API
   *            rejects is contained as an untrackable key
   * @param maxAge how old a value may be; defaults to the family's declared planning age. It is
   *               a lookup window only, never declared, and never longer than the storage
   *               retention, past which the store no longer holds values.
   */
  def latest(
      key: => Map[String, DimValue],
      nowMs: Long,
      policy: HistoryPolicy,
      maxAge: Option[Duration] = None): Option[Double] = {
    boundKey(key).flatMap { dims =>
      val current = store()
      if (!declaredIn(current, policy)) {
        None
      } else {
        try {
          val ageMs = math.min(
            maxAge.getOrElse(family.retention.planningMaxAge()).toMillis,
            family.retention.storageRetention().toMillis)
          val request = SummaryRequest.builder(family.metric)
            .bound(dims.asJava)
            .window(nowMs - ageMs, nowMs + 1)
            .limit(1)
            .build()
          summarized(current.summarize(Collections.singletonList(request),
            policy.planningTimeout))
        } catch {
          case t: Throwable if MetricHistory.isContained(t) =>
            abstain(s"lookup-${t.getClass.getName}", s"lookup failed (${t.getClass.getName})")
            None
        }
      }
    }
  }

  /** Fire-and-forget; the store may drop it. */
  def record(
      key: => Map[String, DimValue],
      value: Double,
      atMs: Long,
      policy: HistoryPolicy): Unit = {
    if (MetricHistory.isUsable(value)) {
      boundKey(key).foreach { dims =>
        val current = store()
        if (declaredIn(current, policy)) {
          try {
            current.record(new Observation(family.metric, dims.asJava, value, atMs))
          } catch {
            case t: Throwable if MetricHistory.isContained(t) =>
              abstain(s"record-${t.getClass.getName}", s"record failed (${t.getClass.getName})")
          }
        }
      }
    }
  }

  /** For tests. */
  private[perf] def reportedReasons: Set[String] = reported.asScala.toSet

  private def summarized(responses: JList[SummaryResponse]): Option[Double] = {
    if (responses == null || responses.size() != 1 || responses.get(0) == null ||
        responses.get(0).status() == null) {
      abstain("lookup-malformed", "lookup returned a malformed response")
      None
    } else {
      val response = responses.get(0)
      val status = response.status()
      if (status.code() != Status.Code.OK) {
        abstain(s"lookup-${status.code()}", s"lookup returned ${status.code()}")
        None
      } else {
        // OK with no summary means no evidence, not an error.
        Option(response.summary()).filter(_.count() > 0)
          // limit(1): the mean is the single latest observation.
          .map(_.mean())
          .filter(MetricHistory.isUsable)
      }
    }
  }

  private def declaredIn(current: MetricStore, policy: HistoryPolicy): Boolean = {
    val known = declaration
    if (known.in eq current) {
      known.accepted
    } else {
      synchronized {
        if (!(declaration.in eq current)) {
          declaration = Declaration(current, declareNow(current, policy))
        }
        declaration.accepted
      }
    }
  }

  private def declareNow(current: MetricStore, policy: HistoryPolicy): Boolean = {
    try {
      val statuses =
        current.declare(Collections.singletonList(family.schema), policy.declareTimeout)
      if (statuses == null || statuses.size() != 1 || statuses.get(0) == null) {
        abstain("declare-malformed", "declaration returned a malformed response")
        false
      } else {
        val status = statuses.get(0)
        if (status.code() == SchemaStatus.Code.ACCEPTED) {
          if (status.reason() != null && reported.add("declare-ACCEPTED")) {
            logInfo(s"History metric ${family.name} v${family.version} accepted with a warning")
          }
          true
        } else {
          report(s"declare-${status.code()}",
            s"History metric ${family.name} v${family.version} not accepted " +
              s"(${status.code()}); decisions that need it stay static")
          false
        }
      }
    } catch {
      case t: Throwable if MetricHistory.isContained(t) =>
        abstain(s"declare-${t.getClass.getName}", s"declaration failed (${t.getClass.getName})")
        false
    }
  }

  /**
   * The key's values, or None for a key the family cannot track: a value over the size cap or
   * with malformed characters (reported), values that are not the family's dimensions in name and
   * kind (reported), or an empty string, which names nothing.
   */
  private def boundKey(key: => Map[String, DimValue]): Option[Map[String, DimValue]] = {
    try {
      val dims = key
      val declared = family.dimensions.map(d => d.name() -> d.kind()).toMap
      if (dims.map { case (name, value) => name -> value.kind() } != declared) {
        report("key-mismatch", s"History metric ${family.name} v${family.version}: the key does " +
          "not match the declared dimensions; its decisions stay static")
        None
      } else if (dims.values.exists(v => v.kind() == DimValue.Kind.STRING &&
          v.stringValue().isEmpty)) {
        None
      } else {
        Some(dims)
      }
    } catch {
      case _: IllegalArgumentException =>
        report("untrackable-key", s"History metric ${family.name} v${family.version}: keys " +
          s"over ${DimValue.MAX_CANONICAL_BYTES - 3} UTF-8 bytes or with malformed characters " +
          "are not tracked; their decisions stay static")
        None
    }
  }

  private def abstain(reason: String, what: String): Unit = report(reason,
    s"History metric ${family.name} v${family.version} $what; planning keeps its static decisions")

  private def report(reason: String, message: String): Unit = {
    if (reported.add(reason)) {
      logWarning(message)
    }
  }
}

object MetricHistory {

  /** Finite and positive. */
  def isUsable(value: Double): Boolean = !value.isNaN && !value.isInfinite && value > 0.0d

  /** Same set the provider manager contains. */
  def isContained(t: Throwable): Boolean = HistoryMetricsManager.isContained(t)
}
