/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicLong

// This is similar to Guava ThreadFactoryBuilder
// Avoid to use Guava as it is a messy dependency in practice.
class ThreadFactoryBuilder {
  private var nameFormat = Option.empty[String]
  private var daemon = Option.empty[Boolean]

  def setNameFormat(nameFormat: String): ThreadFactoryBuilder = {
    nameFormat.format(0)
    this.nameFormat = Some(nameFormat)
    this
  }

  def setDaemon(daemon: Boolean): ThreadFactoryBuilder = {
    this.daemon = Some(daemon)
    this
  }

  def build(): ThreadFactory = {
    val count = nameFormat.map(_ => new AtomicLong(0))
    new ThreadFactory() {
      private val defaultThreadFactory = Executors.defaultThreadFactory

      override def newThread(r: Runnable): Thread = {
        val thread = defaultThreadFactory.newThread(r)
        nameFormat.foreach(f => thread.setName(f.format(count.get.getAndIncrement())))
        daemon.foreach(b => thread.setDaemon(b))
        thread
      }
    }
  }
}
