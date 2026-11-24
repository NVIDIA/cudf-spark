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

package org.apache.spark.shuffle.sort.io

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files

import com.nvidia.spark.rapids.ShimLoader
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.shuffle.IndexShuffleBlockResolver

class RapidsLocalDiskShuffleMapOutputWriterSuite extends AnyFunSuite with MockitoSugar {
  test("channel writes report partition bytes before the partial file is finished") {
    val directory = Files.createTempDirectory("shuffle-channel-lengths-").toFile
    val blockResolver = mock[IndexShuffleBlockResolver]
    when(blockResolver.getDataFile(1, 2L)).thenReturn(new File(directory, "shuffle.data"))
    val writer = new RapidsLocalDiskShuffleMapOutputWriter(
      1, 2L, 2, blockResolver, new SparkConf(false))
    writer.setForceFileOnlyMode()

    try {
      val firstPartition = writer.getPartitionWriter(0)
      val channelWrapper = firstPartition.openChannelWrapper().get()
      val channel = channelWrapper.channel()
      assert(channel.write(ByteBuffer.wrap(Array[Byte](1, 2, 3))) == 3)
      assert(channelWrapper.channel().write(ByteBuffer.wrap(Array[Byte](4, 5))) == 2)
      assert(firstPartition.getNumBytesWritten == 5)
      channel.close()
      channelWrapper.close()
      assert(writer.getPartitionLengths().sameElements(Array[Long](5, 0)))

      val secondPartition = writer.getPartitionWriter(1)
      val secondChannelWrapper = secondPartition.openChannelWrapper().get()
      val secondChannel = secondChannelWrapper.channel()
      assert(secondChannel.write(ByteBuffer.wrap(Array[Byte](6, 7))) == 2)
      assert(secondPartition.getNumBytesWritten == 2)
      secondChannel.close()
      secondChannelWrapper.close()
      assert(writer.getPartitionLengths().sameElements(Array[Long](5, 2)))

      writer.finishWritePhase()
      assert(writer.getPartialFileHandle().get.getTotalBytesWritten == 7)
    } finally {
      writer.abort(null)
      directory.delete()
    }
  }

  test("transfer-to shuffle retains records with RAPIDS local disk IO") {
    val conf = new SparkConf(false)
      .setMaster("local[2]")
      .setAppName("RapidsLocalDiskShuffleMapOutputWriterSuite")
      .set("spark.shuffle.manager", ShimLoader.getRapidsShuffleManagerClass)
      .set("spark.shuffle.sort.io.plugin.class", classOf[RapidsLocalDiskShuffleDataIO].getName)
      .set("spark.file.transferTo", "true")
      .set("spark.ui.enabled", "false")
    val sparkContext = new SparkContext(conf)

    try {
      val shuffled = sparkContext.parallelize(0 until 1000, 4)
        .map(value => (value % 2, value))
        .partitionBy(new HashPartitioner(2))
      assert(shuffled.count() == 1000)
    } finally {
      sparkContext.stop()
    }
  }
}
