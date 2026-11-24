/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

/**
 * The each Avro block information
 *
 * @param blockStart the start of block
 * @param blockSize  the whole block size = the size between two sync buffers + sync buffer
 * @param dataSize   the block data size
 * @param count      how many entries in this block
 */
case class BlockInfo(blockStart: Long, blockSize: Long, dataSize: Long, count: Long)

/**
 * The mutable version of the BlockInfo without block start.
 * This is for reusing an existing instance when accessing data in the iterator pattern.
 *
 * @param blockSize the whole block size (the size between two sync buffers + sync buffer size)
 * @param dataSize  the data size in this block
 * @param count   how many entries in this block
 */
case class MutableBlockInfo(var blockSize: Long, var dataSize: Long, var count: Long)
