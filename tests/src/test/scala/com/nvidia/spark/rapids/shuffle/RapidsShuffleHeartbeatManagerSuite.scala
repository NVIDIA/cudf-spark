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

package com.nvidia.spark.rapids.shuffle

import com.nvidia.spark.rapids.{RapidsExecutorUpdateMsg, RapidsShuffleHeartbeatEndpoint, RapidsShuffleHeartbeatHandler, RapidsShuffleHeartbeatManager}
import org.mockito.Mockito._

import org.apache.spark.sql.rapids.execution.TrampolineUtil

class RapidsShuffleHeartbeatManagerSuite extends RapidsShuffleTestHelper {
  test("adding an executor") {
    val hbMgr = new RapidsShuffleHeartbeatManager(1000, 2000)
    val updateMsg = hbMgr.registerExecutor(
      TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123")))
    assertResult(0)(updateMsg.ids.length)
  }

  test("a heartbeat picks up new executors") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // so executors don't timeout

    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // first executor
    hbMgr.registerExecutor(exec1)
    // second executor
    hbMgr.registerExecutor(exec2)

    // first executor heartbeat (finding out about executor 2)
    hbMgr.executorHeartbeat(exec1) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(1)(idAndExecutorData.length)
        val peerBlockManager = idAndExecutorData.head
        assertResult(exec2)(peerBlockManager)
    }
  }

  test("a subsequent heartbeat with no new executors is empty") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // so executors don't timeout

    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // first executor
    hbMgr.registerExecutor(exec1)
    // second executor
    hbMgr.registerExecutor(exec2)

    // first executor heartbeat (finding out about executor 2)
    val firstUpdate = hbMgr.executorHeartbeat(exec1)
    val peerBlockManager = firstUpdate.ids.head
    assertResult(exec2)(peerBlockManager)

    // second heartbeat from exec1, should be empty
    val secondUpdate = hbMgr.executorHeartbeat(exec1)
    assertResult(0)(secondUpdate.ids.length)

    // first heartbeat from exec2 returns empty, it already knew about exec1 on registration
    val firstUpdateExec2 = hbMgr.executorHeartbeat(exec2)
    assertResult(0)(firstUpdateExec2.ids.length)
  }

  test("an executor tells heartbeat handler about new peers") {
    // updatePeers does not need a live endpoint. Avoid constructing one because its executor
    // thread factory correctly requires the executor plugin to have selected a GPU device.
    val hbEndpoint = org.mockito.Mockito.mock(
      classOf[RapidsShuffleHeartbeatEndpoint], CALLS_REAL_METHODS)
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val hbHandler = mock[RapidsShuffleHeartbeatHandler]
    hbEndpoint.updatePeers(hbHandler, Seq(exec1))
    verify(hbHandler, times(1)).addPeer(exec1)
  }

  test("0 or negative interval values are invalid") {
    assertThrows[IllegalArgumentException] {
      new RapidsShuffleHeartbeatManager(0, 0)
    }
    assertThrows[IllegalArgumentException] {
      new RapidsShuffleHeartbeatManager(-1, 0)
    }
  }

  test("the heartbeat timeout should be greater than the interval") {
    assertThrows[IllegalArgumentException] {
      new RapidsShuffleHeartbeatManager(1000, -1)
    }
    assertThrows[IllegalArgumentException] {
      new RapidsShuffleHeartbeatManager(1000, 500)
    }
    assertThrows[IllegalArgumentException] {
      new RapidsShuffleHeartbeatManager(1000, 1000)
    }
    new RapidsShuffleHeartbeatManager(1000, 1500)
    new RapidsShuffleHeartbeatManager(1000, 2000)
  }

  test("an executor that hasn't heartbeated exactly at 2x interval is still alive") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // startup time for executorId 1 was 1000
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer1", 456, Some("rapids=456"))
    val res1 = hbMgr.registerExecutor(exec1)
    // get nothing back, since the only executor is executor 1
    assertResult(0)(res1.ids.length)

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(3000) // getCurrentTime while registering executorId 2
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // exec1 is NOT stale since 3000 - 1000 == 1000 * 2.
    // When executor 2 registers we should get exec1 back
    val res2 = hbMgr.registerExecutor(exec2)
    assertResult(1)(res2.ids.length)
    assertResult("1")(res2.ids.head.executorId)
  }

  test("an executor that hasn't heartbeated within 2x interval") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // startup time for executorId 1 was 1000
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer1", 456, Some("rapids=456"))
    val res1 = hbMgr.registerExecutor(exec1)
    // get nothing back, since the only executor is executor 1
    assertResult(0)(res1.ids.length)

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(3001) // getCurrentTime while registering executorId 2
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // exec1 is stale since 3001 - 1000 > 1000 * 2.
    // When executor 2 registers we clean up exec1, so exec2 is not told about exec1
    val res2 = hbMgr.registerExecutor(exec2)
    assertResult(0)(res2.ids.length)
  }

  test("A heartbeat within the timeout keeps the executor alive") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // startup time for executorId 1 was 1000
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer1", 456, Some("rapids=456"))
    val res1 = hbMgr.registerExecutor(exec1)
    // get nothing back, since the only executor is executor 1
    assertResult(0)(res1.ids.length)

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(2000) // getCurrentTime while registering executorId 2
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))
    val res2 = hbMgr.registerExecutor(exec2)
    // get executor 1, since it's still valid
    assertResult(1)(res2.ids.length)
    assertResult("1")(res2.ids.head.executorId)

    // lets heartbeat, we should get executor 2 since we hadn't seen it at registration
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(4000)
    hbMgr.executorHeartbeat(exec1) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(1)(idAndExecutorData.length)
        val peerBlockManager = idAndExecutorData.head
        assertResult(exec2)(peerBlockManager)
    }

    // lets heartbeat again, we should NOT get executor 2
    // since we saw it in the last heartbeat
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(4000)
    hbMgr.executorHeartbeat(exec1) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(0)(idAndExecutorData.length)
    }

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(5000) // getCurrentTime while registering executorId 3 (other executors: 2)
    // executorId 1 and 2 are still not stale
    // executorId 1 = 4000 => ((5000 - 4000) is not > 2 * 1000)
    // executorId 2 = 2000 => ((5000 - 2000) is > 2 * 1000) and so it is stale
    val exec3 = TrampolineUtil.newBlockManagerId("3", "peer3", 456, Some("rapids=456"))
    val res3 = hbMgr.registerExecutor(exec3)
    // get executor 1, since it's the only one that is valid
    assertResult(1)(res3.ids.length)
    assertResult("1")(res3.ids.head.executorId)
  }

  test("expired executors can heartbeat and rejoin") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // startup time for executorId 1 was 1000
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer1", 456, Some("rapids=456"))
    val res1 = hbMgr.registerExecutor(exec1)
    // get nothing back, since the only executor is executor 1
    assertResult(0)(res1.ids.length)

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(2000) // getCurrentTime while registering executorId 2
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))
    val res2 = hbMgr.registerExecutor(exec2)
    // get executor 1, since it's still valid
    assertResult(1)(res2.ids.length)
    assertResult("1")(res2.ids.head.executorId)

    // heartbeat exec 2 later, where executor 1 is expired
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(4000)
    hbMgr.executorHeartbeat(exec2) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(0)(idAndExecutorData.length)
    }

    // executor id 1 heartbeats eventually, rejoining
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(5000)
    hbMgr.executorHeartbeat(exec1) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(1)(idAndExecutorData.length)
        assertResult("2")(idAndExecutorData.head.executorId)
    }

    // executor id 2 heartbeats getting exec1 (again)
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(5100)
    hbMgr.executorHeartbeat(exec2) match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(1)(idAndExecutorData.length)
        assertResult("1")(idAndExecutorData.head.executorId)
    }
  }

  test("a heartbeat is equivalent to a registration") {
    val hbMgr = spy(new RapidsShuffleHeartbeatManager(1000, 2000))
    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(1000) // startup time for executorId 1 was 1000
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer1", 456, Some("rapids=456"))
    val res1 = hbMgr.registerExecutor(exec1)
    // get nothing back, since the only executor is executor 1
    assertResult(0)(res1.ids.length)

    when(hbMgr.getCurrentTimeMillis)
      .thenReturn(2000) // getCurrentTime while heartbeating executorId 2
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))
    val res2 = hbMgr.executorHeartbeat(exec2)
    // get executor 1 and now we are registered, even though it was a heartbeat that we received.
    assertResult(1)(res2.ids.length)
    assertResult("1")(res2.ids.head.executorId)
  }
}
