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

import java.io.ByteArrayOutputStream

import org.scalatest.funsuite.AnyFunSuite

class RapidsConfSuite extends AnyFunSuite {

  test("config version metadata is included in table help") {
    var registered: Option[ConfEntry[_]] = None
    val entry = new ConfBuilder("spark.rapids.test.versioned", e => registered = Some(e))
      .doc("test doc")
      .sinceVersion("26.08")
      .stringConf
      .createWithDefault("enabled")

    assert(registered.contains(entry))
    assertResult("26.08")(entry.versionInfo.sinceVersion)

    val out = new ByteArrayOutputStream()
    Console.withOut(out) {
      entry.help(asTable = true)
    }

    assertResult(
      "<a name=\"test.versioned\"></a>spark.rapids.test.versioned|" +
        "test doc|enabled|Runtime|26.08\n") {
      out.toString("UTF-8")
    }
  }

  test("config version metadata defaults to history lookup") {
    val entry = new ConfBuilder("spark.rapids.sql.enabled", _ => ())
      .doc("test doc")
      .booleanConf
      .createWithDefault(false)

    assertResult("0.1.0")(entry.versionInfo.sinceVersion)
  }

  test("unknown config version metadata remains explicit") {
    val entry = new ConfBuilder("spark.rapids.test.unknown", _ => ())
      .doc("test doc")
      .booleanConf
      .createWithDefault(false)

    assertResult(ConfVersionInfo.UNKNOWN_VERSION)(entry.versionInfo.sinceVersion)
  }
}
