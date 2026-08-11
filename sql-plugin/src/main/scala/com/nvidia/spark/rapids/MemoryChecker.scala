/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}

import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

trait MemoryChecker {
  def getAvailableMemoryBytes(rapidsConf: RapidsConf): Option[Long]
}

/**
 * Utility class that queries the runtime environment to determine how much
 * system memory is currently available. It does this by trying to figure out
 * what type of environment it's in (eg. docker, YARN, bare metal, etc.), based
 * on which it checks corresponding files, env variables, etc. for memory usage
 * and limits.
 */
object MemoryCheckerImpl extends MemoryChecker with Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RapidsConf(new SparkConf())
    println(s"Available memory: ${getAvailableMemoryBytes(conf)} bytes")
  }

  def getAvailableMemoryBytes(rapidsConf: RapidsConf): Option[Long] = {
    logInfo("Trying to detect available CPU memory")
    val procLimit = if (isSlurmContainer) {
      logInfo("Slurm environment detected")
      getSlurmMemory
    } else {
      getCgroupMemory(rapidsConf)
    }

    val systemLimit = getHostMemory

    if (procLimit.isEmpty && systemLimit.isEmpty) {
      logWarning("no process or system memory limits detected")
      None
    } else if (procLimit.isEmpty) {
      logInfo(s"no process limits detected; using system limit of ${systemLimit.get}")
      systemLimit
    } else if (systemLimit.isEmpty) {
      logInfo(s"no system limits detected; using process limit of ${procLimit.get}")
      procLimit
    } else {
      logInfo(s"detected system limit of ${systemLimit.get} and process limit of " +
        s"${procLimit.get}")
      val res = Math.min(procLimit.get, systemLimit.get)
      logInfo(s"using the minimum of $res")
      Some(res)
    }
  }

  private def isSlurmContainer: Boolean = {
    System.getenv("SLURM_JOB_ID") != null
  }

  private def fromUTF8File(path: String): BufferedSource = Source.fromFile(path)(Codec.UTF8)

  private def getVmRSS: Option[Long] = {
    val pid = ManagementFactory.getRuntimeMXBean.getName.split("@").head.toInt
    val statusFile = s"/proc/$pid/status"

    val result = Try(withResource(fromUTF8File(statusFile)) { source =>
      source.getLines()
        .find(_.startsWith("VmRSS:"))
        .flatMap { line =>
          line.split("\\s+") match {
            case Array(_, value, _*) => Try(value.toLong).toOption
            case _ => None
          }
        }
    })
    result match {
      case Success(value) => value
      case Failure(exception) => {
        logWarning(s"failed to read $statusFile: $exception")
        None
      }
    }
  }

  private def getSlurmMemory: Option[Long] = {
    val totalOpt = Option(System.getenv("SLURM_MEM_PER_NODE"))
      .map(_.toLong * 1024 * 1024) // Convert MB to bytes

    for {
      total <- totalOpt
      used  <- getVmRSS
    } yield total - used
  }

  private def readFile(path: String): Option[String] = {
    logInfo(s"attempting to read $path")
    try {
      Some(
        withResource(fromUTF8File(path)) { source =>
          source.mkString.trim
        }
      )
    } catch {
      case _: Exception => None
    }
  }

  private def getCgroupMemory(rapidsConf: RapidsConf): Option[Long] = {
    val (limitPath, usagePath) = getCgroupPaths(rapidsConf)
    // The max appears to be 9223372036854710272 in some cases, which would exceed
    // this otherwise somewhat arbitrary limit. It's not clear exactly what number or
    // range should be valid but realistically anything above 1PB could just be treated
    // as "lets try to limit based on host mem instead or not at all"
    val maxMemLimit = 1L * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
    for {
      limitStr <- readFile(limitPath)
      usageStr <- readFile(usagePath)
      if limitStr != "max" && limitStr.toLong < maxMemLimit
      limit = limitStr.toLong
      usage = usageStr.toLong
    } yield limit - usage
  }

  private def getCgroupPaths(rapidsConf: RapidsConf): (String, String) = {
    val (defaultLimitPath, defaultUsagePath) = detectCgroupPaths
    val confLimitPath = rapidsConf.cgroupsMemoryLimitPath
    val confUsagePath = rapidsConf.cgroupsMemoryUsagePath

    val limitPath = confLimitPath.getOrElse(defaultLimitPath)
    val usagePath = confUsagePath.getOrElse(defaultUsagePath)

    (limitPath, usagePath)
  }

  private def detectCgroupPaths: (String, String) = {
    val v2Limit = "/sys/fs/cgroup/memory.max"
    if (Files.exists(Paths.get(v2Limit))) {
      (v2Limit, "/sys/fs/cgroup/memory.current")
    } else {
      ("/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes")
    }
  }

  private def getHostMemory: Option[Long] = {
    val path = "/proc/meminfo"
    logInfo(s"attempting to read $path")
    val result = Try(withResource(fromUTF8File(path)) { source =>
      source.getLines()
        .find(_.startsWith("MemAvailable:"))
        .map(_.split("\\s+")(1).toLong * 1024) // KB to bytes
    })

    result match {
      case Success(value) => value
      case Failure(exception) => {
        logWarning(s"failed to read $path: $exception")
        None
      }
    }

  }
}
