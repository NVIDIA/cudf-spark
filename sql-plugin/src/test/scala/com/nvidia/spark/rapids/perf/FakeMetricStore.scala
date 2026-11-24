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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.history.{BackendInfo, HistoryMetricsApi, MetricSchema, MetricStore,
  MetricVersionId, Observation, SchemaStatus, Status, Summary, SummaryRequest, SummaryResponse}

/**
 * In-memory `MetricStore` following the API contract: declaration gates recording, requests
 * select by exact dimension values within `[from, to)`, and `limit(n)` keeps the n most recent
 * observations, later-recorded first on equal timestamps. Failure modes are switchable.
 */
class FakeMetricStore extends MetricStore {

  /** Outcome of every declaration; ACCEPTED declarations gate recording. */
  @volatile var declareCode: SchemaStatus.Code = SchemaStatus.Code.ACCEPTED
  @volatile var declareReason: String = _

  /** When set, `summarize` answers every request with this error status. */
  @volatile var summarizeError: Option[Status] = None

  /** When set, `summarize` returns this list as is, whatever was asked. */
  @volatile var summarizeRaw: Option[JList[SummaryResponse]] = None

  /** When set, `summarize` throws it. */
  @volatile var summarizeThrows: Option[RuntimeException] = None

  val declareCalls = new ArrayBuffer[(JList[MetricSchema], Duration)]()
  val summarizeCalls = new ArrayBuffer[(SummaryRequest, Duration)]()
  /** Observations the store kept, in recording order. */
  val stored = new ArrayBuffer[Observation]()
  /** Observations offered, kept or not. */
  val offered = new ArrayBuffer[Observation]()

  private val accepted = scala.collection.mutable.Map[MetricVersionId, MetricSchema]()

  override def declare(schemas: JList[MetricSchema], timeout: Duration): JList[SchemaStatus] =
    synchronized {
      declareCalls += ((schemas, timeout))
      schemas.asScala.map { schema =>
        if (declareCode == SchemaStatus.Code.ACCEPTED) {
          accepted(schema.metric()) = schema
          SchemaStatus.accepted(schema.metric(), declareReason)
        } else {
          SchemaStatus.of(schema.metric(), declareCode,
            Option(declareReason).getOrElse("rejected by test"))
        }
      }.asJava
    }

  override def record(observation: Observation): Unit = synchronized {
    offered += observation
    accepted.get(observation.metric()).foreach { schema =>
      val names = schema.dimensions().asScala.map(_.name()).toSet
      if (observation.dimensions().keySet().asScala == names) {
        stored += observation
      }
    }
  }

  override def summarize(requests: JList[SummaryRequest], timeout: Duration)
      : JList[SummaryResponse] = synchronized {
    requests.asScala.foreach(r => summarizeCalls += ((r, timeout)))
    summarizeThrows.foreach(e => throw e)
    summarizeRaw.getOrElse {
      requests.asScala.map { request =>
        summarizeError.map(SummaryResponse.error).getOrElse(answer(request))
      }.asJava
    }
  }

  override def info(): BackendInfo =
    new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "in-memory test store")

  private def answer(request: SummaryRequest): SummaryResponse = {
    if (!accepted.contains(request.metric())) {
      SummaryResponse.error(Status.of(Status.Code.NOT_DECLARED, "not declared"))
    } else {
      val matching = stored.zipWithIndex.filter { case (o, _) =>
        o.metric() == request.metric() &&
          request.bound().asScala.forall { case (k, v) => o.dimensions().get(k) == v } &&
          o.timestampMs() >= request.fromMs() && o.timestampMs() < request.toMs()
      }
      // Most recent first; on equal timestamps the later-recorded one wins.
      val ordered = matching.sortBy { case (o, seq) => (-o.timestampMs(), -seq) }.map(_._1)
      val selected = if (request.limit() > 0) ordered.take(request.limit()) else ordered
      if (selected.isEmpty) {
        SummaryResponse.ok(null)
      } else {
        val values = selected.map(_.value())
        val times = selected.map(_.timestampMs())
        val mean = math.min(values.max, math.max(values.min, values.sum / values.size))
        SummaryResponse.ok(Summary.of(values.size.toLong, mean, values.min, values.max,
          times.min, times.max))
      }
    }
  }
}

object FakeMetricStore {
  def emptyResponses: JList[SummaryResponse] = Collections.emptyList[SummaryResponse]()
}
