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

import java.time.Duration
import java.util.{Locale, ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.nvidia.spark.history.{HistoryMetricsProvider, MetricStores}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * Selects, installs, and owns the configured history metrics provider on the driver.
 *
 * The no-op store remains installed unless one named provider opens and validates successfully.
 */
private[rapids] class HistoryMetricsManager(
    loadProviders: () => Seq[HistoryMetricsProvider] = HistoryMetricsManager.discover _)
    extends Logging {
  private var activeProvider: HistoryMetricsProvider = _
  private var registration: AutoCloseable = _

  def initialize(sparkContext: SparkContext, configuredName: String): Unit = synchronized {
    if (activeProvider != null || registration != null) {
      logWarning("History metrics provider initialization was requested more than once; " +
        "keeping the current provider")
    } else {
      val requested =
        Option(configuredName).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
      if (requested == HistoryMetricsManager.NO_PROVIDER) {
        logInfo("History metrics: requested=none, active=noop, reason=disabled by configuration")
      } else {
        discoverProviders(requested).foreach { providers =>
          select(requested, providers, sparkContext)
        }
      }
    }
  }

  def shutdown(): Unit = synchronized {
    val currentRegistration = registration
    val currentProvider = activeProvider
    registration = null
    activeProvider = null

    if (currentRegistration != null) {
      try {
        currentRegistration.close()
      } catch {
        case failure if HistoryMetricsManager.isContained(failure) =>
          logError("History metrics registration could not be removed", failure)
      }
    }

    if (currentProvider != null) {
      try {
        if (!currentProvider.shutdown(HistoryMetricsManager.PROVIDER_SHUTDOWN_TIMEOUT)) {
          logError(s"History metrics provider ${currentProvider.getClass.getName} " +
            "did not finish shutdown within its budget")
        }
      } catch {
        case failure if HistoryMetricsManager.isContained(failure) =>
          logError(s"History metrics provider ${currentProvider.getClass.getName} " +
            "failed during shutdown", failure)
      }
    }
  }

  private def discoverProviders(
      requested: String): Option[Seq[(String, HistoryMetricsProvider)]] = {
    try {
      val providers = loadProviders().flatMap { provider =>
        providerName(provider).map(_ -> provider)
      }
      val found = if (providers.isEmpty) {
        "none"
      } else {
        providers.map { case (name, provider) =>
          s"$name=${provider.getClass.getName}"
        }.mkString(", ")
      }
      logInfo(s"History metrics provider discovery: requested=$requested, found=$found")
      Some(providers)
    } catch {
      case failure if HistoryMetricsManager.isContained(failure) =>
        fallback(requested, "provider discovery failed; ensure the provider and RAPIDS jars " +
          "use compatible driver classloaders and the provider does not bundle the history " +
          "metrics API", failure)
        None
    }
  }

  private def providerName(provider: HistoryMetricsProvider): Option[String] = {
    try {
      val rawName = provider.name()
      val name = Option(rawName).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
      if (name.isEmpty || name == HistoryMetricsManager.NO_PROVIDER) {
        logError(s"Ignoring history metrics provider ${provider.getClass.getName}: " +
          s"provider name '$rawName' is invalid")
        None
      } else {
        Some(name)
      }
    } catch {
      case failure if HistoryMetricsManager.isContained(failure) =>
        logError(s"Ignoring history metrics provider ${provider.getClass.getName}: " +
          "could not read its name", failure)
        None
    }
  }

  private def select(
      requested: String,
      providers: Seq[(String, HistoryMetricsProvider)],
      sparkContext: SparkContext): Unit = {
    providers.filter(_._1 == requested).map(_._2) match {
      case Seq() =>
        val available = providers.map(_._1).distinct.sorted
        val availableText = if (available.isEmpty) "none" else available.mkString(", ")
        fallback(requested,
          s"provider was not found on the driver classpath; available providers: $availableText")
      case Seq(provider) =>
        open(requested, provider, sparkContext)
      case matches =>
        val implementations = matches.map(_.getClass.getName).mkString(", ")
        fallback(requested, s"multiple providers advertise this name: $implementations")
    }
  }

  private def open(
      requested: String,
      provider: HistoryMetricsProvider,
      sparkContext: SparkContext): Unit = {
    try {
      val store = provider.open(sparkContext)
      val newRegistration = MetricStores.install(store)
      activeProvider = provider
      registration = newRegistration
      logInfo(s"History metrics: requested=$requested, active=$requested, " +
        s"implementation=${provider.getClass.getName}")
    } catch {
      case failure if HistoryMetricsManager.isContained(failure) =>
        cleanupFailedProvider(provider)
        fallback(requested, "provider could not be opened", failure)
    }
  }

  private def cleanupFailedProvider(provider: HistoryMetricsProvider): Unit = {
    try {
      if (!provider.shutdown(HistoryMetricsManager.PROVIDER_SHUTDOWN_TIMEOUT)) {
        logWarning(s"History metrics provider ${provider.getClass.getName} " +
          "did not finish cleanup within its budget")
      }
    } catch {
      case failure if HistoryMetricsManager.isContained(failure) =>
        logWarning(s"History metrics provider ${provider.getClass.getName} also failed cleanup",
          failure)
    }
  }

  private def fallback(requested: String, reason: String, failure: Throwable = null): Unit = {
    val message = s"History metrics: requested=$requested, active=noop, reason=$reason. " +
      "History-backed heuristics remain disabled."
    if (failure == null) {
      logError(message)
    } else {
      logError(message, failure)
    }
  }
}

private[rapids] object HistoryMetricsManager {
  val NO_PROVIDER: String = "none"
  val PROVIDER_SHUTDOWN_TIMEOUT: Duration = Duration.ofSeconds(10)

  def discover(): Seq[HistoryMetricsProvider] = {
    val loader = getClass.getClassLoader
    ServiceLoader.load(classOf[HistoryMetricsProvider], loader).iterator().asScala.toSeq
  }

  def isContained(failure: Throwable): Boolean = {
    NonFatal(failure) ||
      failure.isInstanceOf[LinkageError] ||
      failure.isInstanceOf[ServiceConfigurationError]
  }
}
