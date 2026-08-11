/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TestHashedPriorityQueue {
  private static class TestObj {
    final String name;
    long priority;

    TestObj(String name, long priority) {
      this.name = name;
      this.priority = priority;
    }

    @Override
    public String toString() {
      return "(" + name + "," + priority + ")";
    }
  }

  private static class TestObjPriorityComparator implements Comparator<TestObj> {
    @Override
    public int compare(TestObj o1, TestObj o2) {
      return Long.compare(o1.priority, o2.priority);
    }
  }

  private ArrayList<TestObj> buildTestObjs(int size) {
    ArrayList<TestObj> objs = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      objs.add(new TestObj(Integer.toString(i), i));
    }
    return objs;
  }

  private void insertIntoQueue(HashedPriorityQueue<TestObj> q, Iterator<TestObj> objs) {
    final int originalSize = q.size();
    int i = 0;
    while (objs.hasNext()) {
      assertTrue(q.offer(objs.next()));
      i += 1;
      assertEquals(originalSize + i, q.size());
    }
  }

  private void insertIntoQueueAssertSizeDidntChange(
      HashedPriorityQueue<TestObj> q, Iterator<TestObj> objs) {
    final int originalSize = q.size();
    while (objs.hasNext()) {
      assertFalse(q.offer(objs.next()));
    }
    assertEquals(originalSize, q.size());
  }

  private ArrayList<TestObj> drainQueue(HashedPriorityQueue<TestObj> q) {
    final int originalSize = q.size();
    ArrayList<TestObj> objs = new ArrayList<>(originalSize);
    while (q.peek() != null) {
      TestObj obj = q.poll();
      assertNotNull(obj);
      objs.add(obj);
      assertEquals(originalSize - objs.size(), q.size());
    }
    assertTrue(q.isEmpty());
    return objs;
  }

  @Test
  public void testEmpty() {
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    assertTrue(q.isEmpty());
    assertEquals(0, q.size());
    assertNull(q.poll());
    assertNull(q.peek());
  }

  @Test
  public void testHeapInsertInOrder() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    insertIntoQueue(q, objs.iterator());
    insertIntoQueueAssertSizeDidntChange(q, objs.iterator());
    ArrayList<TestObj> result = drainQueue(q);
    assertEquals(objs, result);
  }

  @Test
  public void testHeapInsertDuplicate() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    insertIntoQueue(q, objs.iterator());
    ArrayList<TestObj> result = drainQueue(q);
    assertEquals(objs, result);
  }

  @Test
  public void testClear() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    insertIntoQueue(q, objs.iterator());
    q.clear();
    assertEquals(0, q.size());
    for (TestObj obj: objs) {
      assertFalse(q.contains(obj));
    }
  }

  @Test
  public void testHeapInsertReverseOrder() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    ArrayList<TestObj> reversedObjs = new ArrayList<>(objs);
    Collections.reverse(reversedObjs);
    insertIntoQueue(q, reversedObjs.iterator());
    ArrayList<TestObj> result = drainQueue(q);
    assertEquals(objs, result);
  }

  @Test
  public void testHeapInsertRandomOrder() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    Random rnd = new Random(0);
    ArrayList<TestObj> shuffled = new ArrayList<>(objs);
    Collections.shuffle(shuffled, rnd);
    insertIntoQueue(q, shuffled.iterator());
    ArrayList<TestObj> result = drainQueue(q);
    assertEquals(objs, result);
  }

  @Test
  public void testHeapMixedOfferPoll() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    Random rnd = new Random(0);
    ArrayList<TestObj> shuffled = new ArrayList<>(objs);
    Collections.shuffle(shuffled, rnd);
    Iterator<TestObj> iterator = shuffled.iterator();
    ArrayList<TestObj> inserted = new ArrayList<>(objs.size());
    ArrayList<TestObj> popped = new ArrayList<>(objs.size());
    while (iterator.hasNext() || !q.isEmpty()) {
      boolean shouldInsert = false;
      if (iterator.hasNext() && !q.isEmpty()) {
        shouldInsert = rnd.nextBoolean();
      } else if (q.isEmpty()) {
        shouldInsert = true;
      }
      if (shouldInsert) {
        // check the results that have been pulled so far
        if (!popped.isEmpty()) {
          inserted.sort(new TestObjPriorityComparator());
          List<TestObj> subset = inserted.subList(0, popped.size());
          assertEquals(subset, popped);
          inserted = new ArrayList<>(inserted.subList(popped.size(), inserted.size()));
          popped.clear();
        }
        TestObj obj = iterator.next();
        inserted.add(obj);
        assertTrue(q.offer(obj));
      } else {
        TestObj obj = q.poll();
        assertNotNull(obj);
        popped.add(obj);
      }
    }
    if (!popped.isEmpty()) {
      inserted.sort(new TestObjPriorityComparator());
      assertEquals(inserted, popped);
    }
  }

  @Test
  public void testMixedRemovalAndPoll() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    Random rnd = new Random(0);
    ArrayList<TestObj> shuffled = new ArrayList<>(objs);
    Collections.shuffle(shuffled, rnd);
    insertIntoQueue(q, shuffled.iterator());
    Collections.shuffle(shuffled, rnd);
    ArrayList<TestObj> popped = new ArrayList<>(objs.size());
    while (!q.isEmpty()) {
      boolean shouldPoll = rnd.nextBoolean();
      if (shouldPoll) {
        TestObj obj = q.poll();
        assertNotNull(obj);
        popped.add(obj);
        assertTrue(shuffled.remove(obj));
      } else {
        TestObj obj = shuffled.remove(shuffled.size() - 1);
        assertTrue(q.remove(obj));
      }
    }
    ArrayList<TestObj> sorted = new ArrayList<>(popped);
    sorted.sort(new TestObjPriorityComparator());
    assertEquals(sorted, popped);
  }

  @Test
  public void testUpdatePriority() {
    ArrayList<TestObj> objs = buildTestObjs(100);
    HashedPriorityQueue<TestObj> q =
        new HashedPriorityQueue<>(new TestObjPriorityComparator());
    Random rnd = new Random(0);
    Collections.shuffle(objs, rnd);
    insertIntoQueue(q, objs.iterator());
    for (TestObj obj : objs) {
      obj.priority = rnd.nextLong();
      q.priorityUpdated(obj);
    }
    objs.sort(new TestObjPriorityComparator());
    ArrayList<TestObj> result = drainQueue(q);
    assertEquals(objs, result);
  }
}
