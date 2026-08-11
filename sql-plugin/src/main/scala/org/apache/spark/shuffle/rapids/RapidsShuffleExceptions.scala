/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package org.apache.spark.shuffle.rapids

import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId

class RapidsShuffleFetchFailedException(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Long,
    mapIndex: Int,
    reduceId: Int,
    message: String,
    cause: Throwable)
    extends FetchFailedException(
      bmAddress, shuffleId, mapId, mapIndex, reduceId, message, cause) {
}

class RapidsShuffleTimeoutException(message: String) extends Exception(message)

/**
 * Internal exception thrown by `BufferSendState` in case where it detects
 * an `IOException` when copying buffers into a bounce buffer that it is preparing.
 * @param message - string describing the issue
 * @param cause - causing exception, or null
 */
class RapidsShuffleSendPrepareException(message: String, cause: Throwable)
    extends Exception(message, cause)
