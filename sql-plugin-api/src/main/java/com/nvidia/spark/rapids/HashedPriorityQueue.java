/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Implements a priority queue based on a heap. Like many priority queue
 * implementations, this provides logarithmic time for inserting elements
 * and removing the top element. However unlike many implementations,
 * this provides logarithmic rather than linear time for the random-access
 * `contains` and `remove` methods. The queue also provides a
 * mechanism for updating the heap after an element's priority has changed
 * via the `priorityUpdated` method instead of requiring the element
 * to be removed and re-inserted.
 *
 * <p>The queue is <strong>NOT thread-safe</strong>.
 *
 * <p>The iterator <strong>does NOT</strong> return elements in priority
 * order.
 *
 * <p>If you want an ordered iterator you need to call priorityIterator.
 */
public final class HashedPriorityQueue<T> extends AbstractQueue<T> {
  private static final int DEFAULT_INITIAL_HEAP_SIZE = 16;
  private final Comparator<? super T> comparator;

  /**
   * An array-based heap. Given a node at index X, the indices of neighboring
   * nodes can be computed via the following:<ul>
   * <li>parent node is at (X-1)/2</li>
   * <li>left child is at 2*X+1</li>
   * <li>right child is at 2*X+2</li></ul>
   */
  private T[] heap;
  private int size;

  /**
   * The map of objects to their location in the heap array.
   * MutableInt is used to reduce garbage creation during sifting.
   */
  private final HashMap<T, MutableInt> locationMap = new HashMap<>();

  public HashedPriorityQueue(Comparator<? super T> comparator) {
    this(DEFAULT_INITIAL_HEAP_SIZE, comparator);
  }

  @SuppressWarnings("unchecked")
  public HashedPriorityQueue(int initialHeapSize, Comparator<? super T> comparator) {
    this.comparator = comparator;
    heap = (T[]) new Object[initialHeapSize];
    size = 0;
  }

  private HashedPriorityQueue(T[] heap, int size, HashMap<T, MutableInt> locationMap,
                              Comparator<? super T> comparator) {
    this.comparator = comparator;
    this.heap = Arrays.copyOf(heap, size);
    this.size = size;
    locationMap.forEach((T k, MutableInt v) -> {
      this.locationMap.put(k, new MutableInt(v.intValue()));
    });
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean offer(T obj) {
    ensureCapacityToInsert();
    MutableInt location = new MutableInt(size);
    MutableInt oldLocation = locationMap.putIfAbsent(obj, location);
    if (oldLocation == null) {
      // Inserted a new object since we didn't have a prior location.
      // Start with the new object at the bottom of the heap and sift it up
      // until heap properties are restored.
      size += 1;
      siftUp(obj, location);
      return true;
    } else {
      // we return false in the case where the object is already
      // in the locationMap
      return false;
    }
  }

  @Override
  public T poll() {
    if (size == 0) {
      return null;
    }
    T result = heap[0];
    if (locationMap.remove(result) == null) {
      throw new IllegalStateException("Object in heap without an index: " + result);
    }
    fillHoleWithLast(0);
    return result;
  }

  @Override
  public T peek() {
    return heap[0];
  }

  @Override
  public boolean contains(Object obj) {
    return locationMap.containsKey(obj);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o) {
    T obj = (T) o;
    MutableInt location = locationMap.remove(obj);
    if (location == null) {
      return false;
    }
    int heapIndex = location.intValue();
    fillHoleWithLast(heapIndex);
    return true;
  }

  @Override
  public void clear() {
    Arrays.fill(heap, 0, size, null);
    locationMap.clear();
    size = 0;
  }

  @Override
  public Object[] toArray() {
    return Arrays.copyOf(heap, size);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T1> T1[] toArray(T1[] a) {
    if (a.length > size) {
      System.arraycopy(heap, 0, a, 0, size);
      return a;
    }
    return (T1[]) Arrays.copyOf(heap, size, a.getClass());
  }

  /**
   * NOTE: This iterator <strong>DOES NOT</strong> iterate elements
   * in priority order.
   */
  @Override
  public Iterator<T> iterator() {
    return Arrays.asList(Arrays.copyOf(heap, size)).iterator();
  }

  public Iterator<T> priorityIterator() {
    HashedPriorityQueue<T> copy = new HashedPriorityQueue<>(heap, size, locationMap, comparator);
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return !copy.isEmpty();
      }

      @Override
      public T next() {
        return copy.poll();
      }
    };
  }

  /**
   * When an object in the heap changes priority, this must be called to
   * restore proper ordering of the heap. After an object's priority
   * changes, this method must be called before any elements are added
   * or removed from the priority queue.
   */
  public void priorityUpdated(T obj) {
    MutableInt location = locationMap.get(obj);
    if (location != null) {
      // Try sifting up first since that operation is cheaper.
      if (!siftUp(obj, location)) {
        siftDown(obj, location);
      }
    }
  }

  /** Given an index of a node, return the index of the node's parent. */
  private int getParentIndex(int heapIndex) {
    return (heapIndex - 1) >>> 1;
  }

  private MutableInt getLocation(T obj) {
    MutableInt location = locationMap.get(obj);
    if (location == null) {
      throw new IllegalStateException("Object in heap without a corresponding index: " + obj);
    }
    return location;
  }

  private void updateHeapIndex(T obj, int heapIndex) {
    updateHeapIndex(obj, getLocation(obj), heapIndex);
  }

  private void updateHeapIndex(T obj, MutableInt location, int heapIndex) {
    heap[heapIndex] = obj;
    location.setValue(heapIndex);
  }

  /**
   * Pop the last element off of the heap for placement elsewhere.
   * The heap must not be empty when this is called, and it is the
   * responsibility of the caller to update the index map for where
   * the resulting object is ultimately placed in the heap.
   */
  private T popLastElement() {
    size -= 1;
    T obj = heap[size];
    heap[size] = null;
    return obj;
  }

  private void ensureCapacityToInsert() {
    if (heap.length == Integer.MAX_VALUE) {
      throw new OutOfMemoryError("heap exceeded maximum array size");
    }
    int needed = size + 1;
    if (heap.length <= needed) {
      long newSize = Math.min((long) heap.length * 2, Integer.MAX_VALUE);
      heap = Arrays.copyOf(heap, (int) newSize);
    }
  }

  /**
   * Perform a heap sift-up operation. The specified object is stored into
   * the heap at its location after the sift-up operation completes.
   *
   * @param obj object being sifted
   * @param location mutable current location that will be updated
   * @return true if the object location was updated
   */
  private boolean siftUp(T obj, MutableInt location) {
    boolean sifted = false;
    int heapIndex = location.intValue();
    while (heapIndex > 0) {
      int parentIndex = getParentIndex(heapIndex);
      T parent = heap[parentIndex];
      if (comparator.compare(obj, parent) >= 0) {
        break;
      }

      sifted = true;
      updateHeapIndex(parent, heapIndex);
      heapIndex = parentIndex;
    }

    updateHeapIndex(obj, location, heapIndex);
    return sifted;
  }

  /**
   * Perform a heap sift-down operation. The specified object is stored into
   * the heap at its location after the sift-down operation completes.
   *
   * @param obj object being sifted
   * @param location mutable current location that will be updated
   * @return true if the object location was updated
   */
  private boolean siftDown(T obj, MutableInt location) {
    boolean sifted = false;
    int heapIndex = location.intValue();
    final int parentIndexEnd = getParentIndex(size + 1);
    while (heapIndex < parentIndexEnd) {
      final int leftChildIndex = 2 * heapIndex + 1;
      final int rightChildIndex = leftChildIndex + 1;
      T leastChild = heap[leftChildIndex];
      int leastChildIndex = leftChildIndex;
      if (rightChildIndex < size) {
        T rightChild = heap[rightChildIndex];
        if (comparator.compare(leastChild, rightChild) > 0) {
          leastChild = rightChild;
          leastChildIndex = rightChildIndex;
        }
      }

      if (comparator.compare(obj, leastChild) <= 0) {
        break;
      }

      sifted = true;
      updateHeapIndex(leastChild, heapIndex);
      heapIndex = leastChildIndex;
    }

    updateHeapIndex(obj, location, heapIndex);
    return sifted;
  }

  /**
   * Move the last element into the specified index to fill a hole
   * created by removing another object, then sift to restore the heap.
   */
  private void fillHoleWithLast(int holeIndex) {
    // Pop off the last element in the array to fill the hole.
    // The element likely has a large value, so try sifting down first.
    T obj = popLastElement();
    if (holeIndex < size) {
      MutableInt location = getLocation(obj);
      location.setValue(holeIndex);
      if (!siftDown(obj, location)) {
        siftUp(obj, location);
      }
    }
  }
}
