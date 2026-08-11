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

import java.io.OutputStream

import org.apache.avro.file.DataFileConstants
import org.apache.avro.io.EncoderFactory

/**
 * AvroDataWriter, used to write a avro file header to the output stream.
 */
class AvroFileWriter(os: OutputStream) {

  private val vout = new EncoderFactory().directBinaryEncoder(os, null)

  final def writeHeader(header: Header): Unit = {
    val meta = header.meta
    // 1) write magic
    vout.writeFixed(DataFileConstants.MAGIC)
    // 2) write metadata
    vout.writeMapStart()
    vout.setItemCount(meta.size)
    meta.foreach{ case (key, value) =>
      vout.startItem()
      vout.writeString(key)
      vout.writeBytes(value)
    }
    vout.writeMapEnd()
    // 3) write initial sync
    vout.writeFixed(header.sync)
    vout.flush()
  }
}

object AvroFileWriter {

  def apply(os: OutputStream): AvroFileWriter = new AvroFileWriter(os)
}
