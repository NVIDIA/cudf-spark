/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.commons.lang3.mutable.MutableLong

import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{ProxyRapidsShuffleInternalManagerBase, RapidsShuffleInternalManagerBase}
import org.apache.spark.storage.BlockManagerId

class RapidsShuffleHeartbeatManager(heartbeatIntervalMillis: Long,
                                    heartbeatTimeoutMillis: Long) extends Logging {
  require(heartbeatIntervalMillis > 0,
    s"The interval value: $heartbeatIntervalMillis ms is not > 0")

  require(heartbeatTimeoutMillis > heartbeatIntervalMillis,
    s"The interval $heartbeatIntervalMillis ms and timeout $heartbeatTimeoutMillis ms " +
      s"values are invalid." +
      s"${RapidsConf.SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_TIMEOUT.key} " +
      s"Must be set higher than " +
      s"${RapidsConf.SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL.key}.")

  // exposed so that it can be mocked in the tests
  def getCurrentTimeMillis: Long = System.currentTimeMillis()

  private case class ExecutorRegistration(
      id: BlockManagerId,
      // this is this executor's registration order, as given by this manager
      registrationOrder: Long,
      // this is the last registration order this executor is aware of overall
      lastRegistrationOrderSeen: MutableLong,
      // last heartbeat received from this executor in millis
      lastHeartbeatMillis: MutableLong)

  // a counter used to mark each new executor registration with an order
  var registrationOrder = 0L

  // Executors ordered from most recently registered to least-recently registered
  private[this] var executors = new ArrayBuffer[ExecutorRegistration]()

  // A mapping of executor IDs to its registration, populated in `registerExecutor`
  private[this] val executorRegistrations =
    new java.util.HashMap[BlockManagerId, ExecutorRegistration]

  // Min-heap with the root node being the executor that least recently heartbeated
  private[this] val leastRecentHeartbeat =
    new HashedPriorityQueue[BlockManagerId]((b1, b2) => {
      val e1 = executorRegistrations.get(b1)
      val e2 = executorRegistrations.get(b2)
      e1.lastHeartbeatMillis.compareTo(e2.lastHeartbeatMillis)
  })

  /**
   * Called by the driver plugin to handle a new registration from an executor.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with all known executors.
   */
  def registerExecutor(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    logDebug(s"Registration from RAPIDS executor at $id")
    require(!executorRegistrations.containsKey(id), s"Executor $id already registered")
    removeDeadExecutors(getCurrentTimeMillis)
    val allExecutors = executors.map(e => e.id).toArray
    val newReg = ExecutorRegistration(id,
      registrationOrder,
      new MutableLong(registrationOrder),
      new MutableLong(getCurrentTimeMillis))
    registrationOrder = registrationOrder + 1
    executors.append(newReg)
    executorRegistrations.put(id, newReg)
    leastRecentHeartbeat.offer(id)
    RapidsExecutorUpdateMsg(allExecutors)
  }

  /**
   * Called by the driver plugin to handle an executor heartbeat.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with new executors, since the last heartbeat was received.
   */
  def executorHeartbeat(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    removeDeadExecutors(getCurrentTimeMillis)
    val lastRegistration = executorRegistrations.get(id)
    if (lastRegistration == null) {
      logDebug(s"Heartbeat from unknown executor $id")
      registerExecutor(id)
    } else {
      val newExecutors = new ArrayBuffer[BlockManagerId]
      val iter = executors.reverseIterator
      var done = false
      while (iter.hasNext && !done) {
        val entry = iter.next()

        if (entry.registrationOrder >= (lastRegistration.lastRegistrationOrderSeen.getValue:
            @nowarn("msg=getValue in class MutableLong is deprecated"))) {
          if (entry.id != id) {
            logDebug(s"Found new executor (to $id): $entry while handling a heartbeat.")
            newExecutors += entry.id
          }
        } else {
          // We are iterating backwards and have found the last previously inspected executor
          // since `registrationOrder` monotonically increases and follows insertion order.
          // We can stop since all peers below this have been sent to the heartbeating peer.
          done = true
        }
      }

      // update this executor's registration with a new heartbeat time, and that last order
      // from the executors list, indicating the order we should stop at next time
      lastRegistration.lastHeartbeatMillis.setValue(getCurrentTimeMillis)
      (lastRegistration.lastRegistrationOrderSeen.setValue(
        registrationOrder): @nowarn("msg=getValue in class MutableLong is deprecated"))

      // since we updated our heartbeat, update our min-heap
      leastRecentHeartbeat.priorityUpdated(lastRegistration.id)
      RapidsExecutorUpdateMsg(newExecutors.toArray)
    }
  }

  // the timeout amount has elapsed, assume this executor is dead
  private def isStaleHeartbeat(hbMillis: Long, currentTime: Long): Boolean = {
    (currentTime - hbMillis) > heartbeatTimeoutMillis
  }

  // must be called holding the object lock
  private def removeDeadExecutors(currentTime: Long): Unit = {
    val leastRecentHb = leastRecentHeartbeat.peek() // look at the executor that is lagging most
    if (leastRecentHb != null &&
        (isStaleHeartbeat(
          executorRegistrations.get(leastRecentHb).lastHeartbeatMillis.getValue,
          currentTime): @nowarn("msg=getValue in class MutableLong is deprecated"))) {
      // make a new buffer of alive executors and replace the old one
      val aliveExecutors = new ArrayBuffer[ExecutorRegistration]()
      executors.foreach { e =>
        if ((isStaleHeartbeat(e.lastHeartbeatMillis.getValue,
            currentTime): @nowarn("msg=getValue in class MutableLong is deprecated"))) {
          logDebug(s"Stale exec, removing $e")
          executorRegistrations.remove(e.id)
          leastRecentHeartbeat.remove(e.id)
        } else {
          aliveExecutors.append(e)
        }
      }
      executors = aliveExecutors
    }
  }
}

class RapidsShuffleHeartbeatEndpoint(pluginContext: PluginContext, conf: RapidsConf)
  extends Logging with AutoCloseable {
  // Number of milliseconds between heartbeats to driver
  private[this] val heartbeatIntervalMillis =
    conf.shuffleTransportEarlyStartHeartbeatInterval

  private[this] val executorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(
      GpuDeviceManager.wrapThreadFactory(new ThreadFactoryBuilder()
        .setNameFormat("rapids-shuffle-hb")
        .setDaemon(true)
        .build(),
        null,
        () => RmmSpark.removeAllCurrentThreadAssociation()))

  /**
   * Handles both registration and heartbeats. On each tick:
   * - If shuffle manager not available yet, wait for next tick
   * - If not registered, try to register
   * - If registered, send heartbeat
   */
  private class HeartbeatTask(ctx: PluginContext) extends Runnable {
    @volatile private var registered = false
    @volatile private var shuffleManagerOpt: Option[RapidsShuffleInternalManagerBase] = None

    override def run(): Unit = {
      try {
        // Try to get shuffle manager if we don't have it yet
        if (shuffleManagerOpt.isEmpty) {
          val shuffleManager = SparkEnv.get.shuffleManager
          if (shuffleManager == null) {
            logDebug("Shuffle manager not available yet, will retry on next heartbeat")
            return
          }

          shuffleManager match {
            case proxy: ProxyRapidsShuffleInternalManagerBase =>
              shuffleManagerOpt = Some(proxy.getRealImpl
                .asInstanceOf[RapidsShuffleInternalManagerBase])
            case _ =>
              logWarning(s"Unexpected shuffle manager type: ${shuffleManager.getClass}. " +
                "Disabling heartbeat.")
              // Cancel this scheduled task by throwing an exception
              throw new IllegalStateException("Unexpected shuffle manager type")
          }
        }

        shuffleManagerOpt.foreach { rapidsShuffleManager =>
          val serverId = rapidsShuffleManager.getServerId
          if (!registered) {
            // Try to register
            logInfo(s"Registering executor $serverId with driver")
            ctx.ask(RapidsExecutorStartupMsg(serverId)) match {
              case RapidsExecutorUpdateMsg(peers) =>
                updatePeers(rapidsShuffleManager, peers)
                registered = true
              case other =>
                logWarning(s"Unexpected response from driver: $other, " +
                  "will retry on next heartbeat")
            }
          } else {
            // Already registered, send heartbeat
            logTrace("Performing executor heartbeat to driver")
            ctx.ask(RapidsExecutorHeartbeatMsg(serverId)) match {
              case RapidsExecutorUpdateMsg(peers) =>
                updatePeers(rapidsShuffleManager, peers)
              case other =>
                logWarning(s"Unexpected response from driver: $other")
            }
          }
        }
      } catch {
        case t: Throwable =>
          logError("Error during shuffle heartbeat", t)
      }
    }
  }

  def updatePeers(shuffleManager: RapidsShuffleHeartbeatHandler,
      peers: Seq[BlockManagerId]): Unit = {
    peers.foreach { peer =>
      logInfo(s"Updating shuffle manager for new executor $peer")
      shuffleManager.addPeer(peer)
    }
  }

  def registerShuffleHeartbeat(): Unit = {
    // Detect driver mode (local mode) using executor ID instead of shuffle manager
    val executorId = SparkEnv.get.executorId
    if (executorId == "driver") {
      logDebug("Local mode detected. Skipping shuffle heartbeat registration.")
    } else {
      // Start the heartbeat loop - it will handle registration on first successful tick
      executorService.scheduleWithFixedDelay(
        new HeartbeatTask(pluginContext),
        0,
        heartbeatIntervalMillis,
        TimeUnit.MILLISECONDS)
    }
  }

  override def close(): Unit = {
    executorService.shutdown()
  }
}
