/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import org.scalatest.funsuite.AnyFunSuite

class PressureMonitorSuite extends AnyFunSuite {

  private val GcGuard = 0.12
  private val DeviceGuard = 0.9
  private val MaxFactor = 8.0
  private val Inc = PressureMonitor.IncreaseMult
  private val Dec = PressureMonitor.DecayStep

  private def nextFactor(prev: Double, gcFrac: Double, devRatio: Double): Double =
    PressureMonitor.computeNextFactor(prev, gcFrac, devRatio, GcGuard, DeviceGuard,
      Inc, Dec, MaxFactor)

  private def assertClose(actual: Double, expected: Double): Unit =
    assert(math.abs(actual - expected) < 1e-9, s"expected $expected but got $actual")

  test("factor is 1.0 when the feature is disabled (default)") {
    // The monitor is inert unless explicitly enabled via initialize(); factor() must be a no-op.
    assertClose(PressureMonitor.factor(), 1.0)
  }

  test("GC over guard increases the factor multiplicatively") {
    assertClose(nextFactor(1.0, gcFrac = 0.2, devRatio = 0.0), 1.0 * Inc)
    assertClose(nextFactor(2.0, gcFrac = 0.2, devRatio = 0.0), 2.0 * Inc)
  }

  test("device-memory over guard increases the factor (independent of GC)") {
    assertClose(nextFactor(1.0, gcFrac = 0.0, devRatio = 0.95), 1.0 * Inc)
  }

  test("a signal exactly at its guard trips back-pressure (>= semantics)") {
    assertClose(nextFactor(1.0, gcFrac = GcGuard, devRatio = 0.0), 1.0 * Inc)
    assertClose(nextFactor(1.0, gcFrac = 0.0, devRatio = DeviceGuard), 1.0 * Inc)
  }

  test("factor is capped at maxFactor under sustained pressure") {
    // 6.0 * 1.5 = 9.0 would exceed the cap, so it clamps to 8.0.
    assertClose(nextFactor(6.0, gcFrac = 0.5, devRatio = 0.0), MaxFactor)
    // already at the cap stays at the cap.
    assertClose(nextFactor(MaxFactor, gcFrac = 0.5, devRatio = 0.0), MaxFactor)
  }

  test("healthy signals decay the factor additively toward 1.0") {
    assertClose(nextFactor(2.0, gcFrac = 0.05, devRatio = 0.5), 2.0 - Dec)
  }

  test("decay is floored at 1.0 (never below no-back-pressure)") {
    // 1.1 - 0.25 = 0.85 would drop below 1.0, so it clamps to 1.0.
    assertClose(nextFactor(1.1, gcFrac = 0.0, devRatio = 0.0), 1.0)
    assertClose(nextFactor(1.0, gcFrac = 0.0, devRatio = 0.0), 1.0)
  }

  test("a full AIMD trajectory: ramp up under pressure, decay when healthy") {
    // Sustained pressure ramps multiplicatively up to the cap.
    var f = 1.0
    for (_ <- 0 until 10) {
      f = nextFactor(f, gcFrac = 0.3, devRatio = 0.0)
    }
    assertClose(f, MaxFactor)
    // Then health decays it additively back to the 1.0 floor.
    for (_ <- 0 until 100) {
      f = nextFactor(f, gcFrac = 0.0, devRatio = 0.0)
    }
    assertClose(f, 1.0)
  }

  test("scaleLimit divides the host budget by the factor, with a floor of 1") {
    // factor == 1.0 (or below) is a pass-through.
    assert(PressureMonitor.scaleLimit(128L * 1024 * 1024, 1.0) == 128L * 1024 * 1024)
    // factor > 1 shrinks the budget proportionally.
    assert(PressureMonitor.scaleLimit(128L * 1024 * 1024, 2.0) == 64L * 1024 * 1024)
    assert(PressureMonitor.scaleLimit(128L * 1024 * 1024, 8.0) == 16L * 1024 * 1024)
    // never reaches zero.
    assert(PressureMonitor.scaleLimit(4L, 8.0) == 1L)
  }

  test("scaleHostLimit is a pass-through when the feature is disabled") {
    assert(PressureMonitor.scaleHostLimit(128L * 1024 * 1024) == 128L * 1024 * 1024)
  }

  test("RapidsConf wires the adaptive back-pressure defaults") {
    val conf = new RapidsConf(Map.empty[String, String])
    assert(!conf.adaptiveBackpressureEnabled)
    assertClose(conf.adaptiveBackpressureGcGuard, 0.12)
    assertClose(conf.adaptiveBackpressureDeviceGuard, 0.9)
    assert(conf.adaptiveBackpressureSampleMs == 200L)
    assertClose(conf.adaptiveBackpressureMaxFactor, 8.0)
  }

  test("RapidsConf honors adaptive back-pressure overrides") {
    val conf = new RapidsConf(Map(
      RapidsConf.ADAPTIVE_BACKPRESSURE_ENABLED.key -> "true",
      RapidsConf.ADAPTIVE_BACKPRESSURE_GC_GUARD.key -> "0.2",
      RapidsConf.ADAPTIVE_BACKPRESSURE_DEVICE_GUARD.key -> "0.85",
      RapidsConf.ADAPTIVE_BACKPRESSURE_SAMPLE_MS.key -> "500",
      RapidsConf.ADAPTIVE_BACKPRESSURE_MAX_FACTOR.key -> "4.0"))
    assert(conf.adaptiveBackpressureEnabled)
    assertClose(conf.adaptiveBackpressureGcGuard, 0.2)
    assertClose(conf.adaptiveBackpressureDeviceGuard, 0.85)
    assert(conf.adaptiveBackpressureSampleMs == 500L)
    assertClose(conf.adaptiveBackpressureMaxFactor, 4.0)
  }
}
