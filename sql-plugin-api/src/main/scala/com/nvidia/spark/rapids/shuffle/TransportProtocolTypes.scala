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

package com.nvidia.spark.rapids.shuffle

/**
 * Currently supported message types in the transport
 */
object MessageType extends Enumeration {
  /**
   * A message used during startup when establishing
   * a connection to a peer.
   */
  val Control = Value

  /**
   * A client will issue: `MetadataRequest`
   * A server will respond with: `MetadataResponse`
   */
  val MetadataRequest = Value

  /**
   * A client will issue: `TransferRequest`
   * A server will respond with: `TransferResponse`
   */
  val TransferRequest = Value

  /**
   * Messages with this type are shuffle blocks
   */
  val Buffer = Value
}

object TransactionStatus extends Enumeration {
  val NotStarted, InProgress, Complete, Success, Error, Cancelled = Value
}

/**
 * Case class representing stats for a transaction
 * @param txTimeMs amount of time this transaction took
 * @param sendSize amount of bytes sent
 * @param receiveSize amount of bytes received
 * @param sendThroughput send throughput in GB/sec
 * @param recvThroughput recv throughput in GB/sec
 */
case class TransactionStats(txTimeMs: Double,
                            sendSize: Long,
                            receiveSize: Long,
                            sendThroughput: Double,
                            recvThroughput: Double)
