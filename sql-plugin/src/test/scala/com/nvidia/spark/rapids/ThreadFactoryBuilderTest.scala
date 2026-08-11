/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, Executors}

import org.scalatest.funsuite.AnyFunSuite

class ThreadFactoryBuilderTest extends AnyFunSuite {

  test("test thread factory builder") {
    val pool1 = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("thread-pool1-1 %s").setDaemon(true).build())
    try {
      var ret = pool1.submit(new Callable[String] {
        override def call(): String = {
          assert(Thread.currentThread().isDaemon)
          assert(Thread.currentThread().getName == "thread-pool1-1 0")
          ""
        }
      })
      // waits and retrieves the result, if above asserts failed, will get execution exception
      ret.get()
      ret = pool1.submit(() => {
        assert(Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "thread-pool1-1 1")
        ""
      })
      ret.get()
    } finally {
      pool1.shutdown()
    }

    val pool2 = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("pool2-%d").build())
    try {
      var ret = pool2.submit(new Callable[String] {
        override def call(): String = {
          assert(!Thread.currentThread().isDaemon)
          assert(Thread.currentThread().getName == "pool2-0")
          ""
        }
      })
      ret.get()
      ret = pool2.submit(() => {
        assert(!Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "pool2-1")
        ""
      })
      ret.get()
    } finally {
      pool2.shutdown()
    }

    val pool3 = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("pool3-%d").setDaemon(false).build())
    try {
      pool3.submit(new Callable[String] {
        override def call(): String = {
          assert(!Thread.currentThread().isDaemon)
          assert(Thread.currentThread().getName == "pool3-0")
          ""
        }
      }).get()
    } finally {
      pool3.shutdown()
    }
  }
}
