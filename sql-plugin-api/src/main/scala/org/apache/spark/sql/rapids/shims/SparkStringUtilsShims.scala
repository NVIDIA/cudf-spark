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

package org.apache.spark.sql.rapids.shims

import org.apache.commons.lang3.{StringUtils => CommonsLang3StringUtils}

/**
 * Shim for Spark StringUtils functions.
 */
object SparkStringUtilsShims {
  /**
   * This shim method is in response to the following two Spark JIRAs:
   *   1. https://issues.apache.org/jira/browse/SPARK-53004
   *   2. https://issues.apache.org/jira/browse/SPARK-52987
   * Apache Spark 4.1.x is reducing its dependency on Apache Commons Lang3, in this case replacing
   * references to StringUtils.abbreviate with its own implementation in SparkStringUtils.
   * This shim implementation retains the Apache Commons Lang3 implementation.
   * When Spark RAPIDS adds Apache Spark 4.1.x support, a new shim can be created to
   * forward to org.apache.spark.util.SparkStringUtils.abbreviate.
   */
  def abbreviate(str: String, len: Int): String = CommonsLang3StringUtils.abbreviate(str, len)
}