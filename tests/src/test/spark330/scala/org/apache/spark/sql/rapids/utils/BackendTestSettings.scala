/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.nvidia.spark.rapids.TestStats

import org.apache.spark.sql.rapids.utils.RapidsTestConstants.RAPIDS_TEST

abstract class BackendTestSettings {

  private val enabledSuites: java.util.Map[String, SuiteSettings] = new util.HashMap()

  protected def enableSuite[T: ClassTag]: SuiteSettings = {
    val suiteName = implicitly[ClassTag[T]].runtimeClass.getCanonicalName
    if (enabledSuites.containsKey(suiteName)) {
      throw new IllegalArgumentException("Duplicated suite name: " + suiteName)
    }
    val suiteSettings = new SuiteSettings
    enabledSuites.put(suiteName, suiteSettings)
    suiteSettings
  }

  private[utils] def shouldRun(suiteName: String, testName: String): Boolean = {
    if (!enabledSuites.containsKey(suiteName)) {
      return false
    }

    val suiteSettings = enabledSuites.get(suiteName)

    val inclusion = suiteSettings.inclusion.asScala
    val exclusion = suiteSettings.exclusion.asScala

    if (inclusion.isEmpty && exclusion.isEmpty) {
      // default to run all cases under this suite
      return true
    }

    if (inclusion.nonEmpty && exclusion.nonEmpty) {
      // error
      throw new IllegalStateException(
        s"Do not use include and exclude conditions on the same test case: $suiteName:$testName")
    }

    if (inclusion.nonEmpty) {
      // include mode
      val isIncluded = inclusion.exists(_.isIncluded(testName))
      return isIncluded
    }

    if (exclusion.nonEmpty) {
      // exclude mode
      val isExcluded = exclusion.exists(_.isExcluded(testName))
      return !isExcluded
    }

    throw new IllegalStateException("Unreachable code")
  }

  sealed trait ExcludeReason
  // The reason should most likely to be a issue link,
  // or a description like "This simply can't work on GPU".
  // It should never be "unknown" or "need investigation"
  case class KNOWN_ISSUE(reason: String) extends ExcludeReason
  case class ADJUST_UT(reason: String) extends ExcludeReason
  case class WONT_FIX_ISSUE(reason: String) extends ExcludeReason

  protected def getJavaMajorVersion(): Int = {
    val version = System.getProperty("java.version")
    // Allow these formats:
    // 1.8.0_72-ea
    // 9-ea
    // 9
    // 11.0.1
    val versionRegex = """(1\.)?(\d+)([._].+)?""".r
    version match {
      case versionRegex(_, major, _) => major.toInt
      case _ => throw new IllegalStateException(s"Cannot parse java version: $version")
    }
  }

  final protected class SuiteSettings {
    private[utils] val inclusion: util.List[IncludeBase] = new util.ArrayList()
    private[utils] val exclusion: util.List[ExcludeBase] = new util.ArrayList()
    private[utils] val excludeReasons: util.List[ExcludeReason] = new util.ArrayList()

    def include(testNames: String*): SuiteSettings = {
      inclusion.add(Include(testNames: _*))
      this
    }

    def exclude(testNames: String, reason: ExcludeReason, condition: Boolean = true): 
        SuiteSettings = {
      if (condition) {
        exclusion.add(Exclude(testNames))
        excludeReasons.add(reason)
      }
      this
    }

    def includeRapidsTest(testName: String*): SuiteSettings = {
      inclusion.add(IncludeRapidsTest(testName: _*))
      this
    }

    def excludeRapidsTest(testName: String, reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeRapidsTest(testName))
      excludeReasons.add(reason)
      this
    }

    def includeByPrefix(prefixes: String*): SuiteSettings = {
      inclusion.add(IncludeByPrefix(prefixes: _*))
      this
    }

    def excludeByPrefix(prefixes: String, reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeByPrefix(prefixes))
      excludeReasons.add(reason)
      this
    }

    def excludeBySuffix(suffixes: String, reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeBySuffix(suffixes))
      excludeReasons.add(reason)
      this
    }

    def includeRapidsTestsByPrefix(prefixes: String*): SuiteSettings = {
      inclusion.add(IncludeRapidsTestByPrefix(prefixes: _*))
      this
    }

    def excludeRapidsTestsByPrefix(prefixes: String, reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeRadpisTestByPrefix(prefixes))
      excludeReasons.add(reason)
      this
    }

    def excludeRapidsTestsBySuffix(suffixes: String, reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeRadpisTestByPrefix(suffixes))
      excludeReasons.add(reason)
      this
    }

    def includeAllRapidsTests(): SuiteSettings = {
      inclusion.add(IncludeByPrefix(RAPIDS_TEST))
      this
    }

    def excludeAllRapidsTests(reason: ExcludeReason): SuiteSettings = {
      exclusion.add(ExcludeByPrefix(RAPIDS_TEST))
      exclusion.add(ExcludeBySuffix(RAPIDS_TEST))
      excludeReasons.add(reason)
      this
    }
  }

  protected trait IncludeBase {
    def isIncluded(testName: String): Boolean
  }

  protected trait ExcludeBase {
    def isExcluded(testName: String): Boolean
  }

  private case class Include(testNames: String*) extends IncludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isIncluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class Exclude(testNames: String*) extends ExcludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isExcluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class IncludeRapidsTest(testNames: String*) extends IncludeBase {
    val nameSet: Set[String] = testNames.map(name => RAPIDS_TEST + name).toSet
    override def isIncluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class ExcludeRapidsTest(testNames: String*) extends ExcludeBase {
    val nameSet: Set[String] = testNames.map(name => RAPIDS_TEST + name).toSet
    override def isExcluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class IncludeByPrefix(prefixes: String*) extends IncludeBase {
    override def isIncluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }

  private case class ExcludeByPrefix(prefixes: String*) extends ExcludeBase {
    override def isExcluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }

  private case class ExcludeBySuffix(suffixes: String*) extends ExcludeBase {
    override def isExcluded(testName: String): Boolean = {
      if (suffixes.exists(suffix => testName.endsWith(suffix))) {
        return true
      }
      false
    }
  }

  private case class IncludeRapidsTestByPrefix(prefixes: String*) extends IncludeBase {
    override def isIncluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(RAPIDS_TEST + prefix))) {
        return true
      }
      false
    }
  }

  private case class ExcludeRadpisTestByPrefix(prefixes: String*) extends ExcludeBase {
    override def isExcluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(RAPIDS_TEST + prefix))) {
        return true
      }
      false
    }
  }
}

object BackendTestSettings {
  val instance: BackendTestSettings = {
    Class
      .forName("org.apache.spark.sql.rapids.utils.RapidsTestSettings")
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[BackendTestSettings]
  }

  def shouldRun(suiteName: String, testName: String): Boolean = {
    val v = instance.shouldRun(suiteName, testName: String)

    if (!v) {
      TestStats.addIgnoreCaseName(testName)
    }

    v
  }
}
