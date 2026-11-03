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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.internal.{Logging, LogKey, MDC}

object DeltaMdcShims {
  // Spark 4.0 constructs MDC entries through the MDC companion object, while Spark 4.1 exposes
  // MDC(LogKey, value) as a method on Logging and no longer provides that companion object.
  // Defining the call inside a Logging implementation lets this shared source compile against
  // both Spark lines.
  private object LoggingBridge extends Logging {
    def createMdc(logKey: LogKey, value: Any): MDC = MDC(logKey, value)
  }

  def mdc(logKey: AnyRef, value: Any): MDC =
    LoggingBridge.createMdc(logKey.asInstanceOf[LogKey], value)
}
