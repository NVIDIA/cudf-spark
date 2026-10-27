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

package com.nvidia.spark.rapids;

import org.apache.spark.sql.execution.SparkPlan;

/** Access Spark's subquery wait for both CPU and GPU plans. */
public final class SparkPlanSubqueries {
  private SparkPlanSubqueries() {}

  public static void waitForSubqueries(SparkPlan plan) {
    // Scala's protected method is public in bytecode. Calling from Java preserves Spark's
    // synchronization and consumes its pending results without reflection or duplicate updates.
    plan.waitForSubqueries();
  }
}
