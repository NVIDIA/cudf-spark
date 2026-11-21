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

package com.nvidia.spark.rapids.fileio.hadoop

import java.io.IOException

import com.nvidia.spark.rapids.fileio.RapidsInputFileProvider
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class HadoopFileIOProviderSuite extends AnyFunSuite {
  private val key = HadoopFileIO.INPUT_FILE_PROVIDER_CLASSES_KEY
  private val first = classOf[FirstTestProvider].getName
  private val second = classOf[SecondTestProvider].getName
  private val failing = classOf[FailingTestProvider].getName
  private val testPath = new Path("rapidstest://bucket/dir/file.parquet")
  private val localPath = new Path("file:/tmp/hadoop-file-io-provider-suite.parquet")

  private def open(path: Path, providers: String): RapidsInputFile = {
    val conf = new Configuration(false)
    if (providers != null) {
      conf.set(key, providers)
    }
    new HadoopFileIO(conf).newInputFile(path)
  }

  private def createdBy(file: RapidsInputFile): String = file match {
    case f: TestInputFile => f.provider
    case other => other.getClass.getSimpleName
  }

  test("no provider configured uses the built-in input file") {
    assert(open(localPath, null).isInstanceOf[HadoopInputFile])
    assert(open(localPath, " ").isInstanceOf[HadoopInputFile])
  }

  test("a provider creates the input file for paths it supports") {
    assertResult("first")(createdBy(open(testPath, first)))
    assertResult(testPath.toString)(open(testPath, first).path())
  }

  test("paths no provider supports fall back to the built-in input file") {
    assert(open(localPath, s"$first,$second").isInstanceOf[HadoopInputFile])
  }

  test("the first provider that supports a path wins") {
    assertResult("first")(createdBy(open(testPath, s"$first,$second")))
    assertResult("second")(createdBy(open(testPath, s"$second,$first")))
  }

  test("blank entries and whitespace in the provider list are ignored") {
    assertResult("second")(createdBy(open(testPath, s" ,$second , $first,")))
  }

  test("providers are instantiated once per class list") {
    val a = HadoopFileIO.providers(s"$first,$second")
    val b = HadoopFileIO.providers(s"$first,$second")
    assert(a.size() == 2)
    assert(a.get(0) eq b.get(0))
    assert(a.get(1) eq b.get(1))
  }

  test("a provider class that cannot be loaded is a clear error") {
    val e = intercept[IllegalArgumentException] {
      open(testPath, "com.example.DoesNotExist")
    }
    assert(e.getMessage.contains("com.example.DoesNotExist"))
    assert(e.getMessage.contains(key))
  }

  test("a class that is not a provider is a clear error") {
    val e = intercept[IllegalArgumentException] {
      open(testPath, classOf[NotAProvider].getName)
    }
    assert(e.getMessage.contains(classOf[NotAProvider].getName))
    assert(e.getMessage.contains(classOf[RapidsInputFileProvider].getName))
  }

  test("an exception from a provider is not swallowed") {
    val e = intercept[IOException] {
      open(testPath, s"$failing,$first")
    }
    assert(e.getMessage.contains("test failure"))
  }
}

class TestInputFile(path: Path, val provider: String) extends RapidsInputFile {
  override def path(): String = path.toString

  override def getLength: Long = 0L

  override def open(): SeekableInputStream = throw new UnsupportedOperationException()
}

abstract class SchemeTestProvider(name: String) extends RapidsInputFileProvider {
  override def supports(path: Path, conf: Configuration): Boolean =
    "rapidstest" == path.toUri.getScheme

  override def create(path: Path, conf: Configuration): RapidsInputFile =
    new TestInputFile(path, name)
}

class FirstTestProvider extends SchemeTestProvider("first")

class SecondTestProvider extends SchemeTestProvider("second")

class FailingTestProvider extends SchemeTestProvider("failing") {
  override def create(path: Path, conf: Configuration): RapidsInputFile =
    throw new IOException("test failure")
}

class NotAProvider
