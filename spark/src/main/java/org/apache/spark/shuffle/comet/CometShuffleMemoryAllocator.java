/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.shuffle.comet;

import java.io.IOException;

import org.apache.spark.errors.SparkCoreErrors;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A simple memory allocator used by `CometShuffleExternalSorter` to allocate memory blocks which
 * store serialized rows. This class is simply an implementation of `MemoryConsumer` that delegates
 * memory allocation to the `TaskMemoryManager`. This requires that the `TaskMemoryManager` is
 * configured with `MemoryMode.OFF_HEAP`, i.e. it is using off-heap memory.
 */
public final class CometShuffleMemoryAllocator extends MemoryConsumer {
  private static CometShuffleMemoryAllocator INSTANCE;

  private final long pageSize;

  public static synchronized CometShuffleMemoryAllocator getInstance(
      TaskMemoryManager taskMemoryManager, long pageSize) {
    if (taskMemoryManager.getTungstenMemoryMode() != MemoryMode.OFF_HEAP) {
      throw new IllegalArgumentException(
          "CometShuffleMemoryAllocator should be used with off-heap "
              + "memory mode, but got "
              + taskMemoryManager.getTungstenMemoryMode());
    }

    if (INSTANCE == null) {
      INSTANCE = new CometShuffleMemoryAllocator(taskMemoryManager, pageSize);
    }

    return INSTANCE;
  }

  CometShuffleMemoryAllocator(TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
    this.pageSize = pageSize;
  }

  public long spill(long l, MemoryConsumer memoryConsumer) throws IOException {
    // JVM shuffle writer does not support spilling for other memory consumers
    return 0;
  }

  public synchronized MemoryBlock allocate(long required) {
    return this.allocatePage(required);
  }

  public synchronized void free(MemoryBlock block) {
    this.freePage(block);
  }

  protected synchronized MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }

    System.out.println("allocatePage: " + page.pageNumber);

    used += page.size();
    return page;
  }

  protected synchronized void freePage(MemoryBlock page) {
    System.out.println("freePage: " + page.pageNumber);

    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }

  public synchronized LongArray allocateArray(long size) {
    long required = size * 8L;
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);

    System.out.println("allocateArray: " + page.pageNumber);

    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += required;
    return new LongArray(page);
  }

  public synchronized void freeArray(LongArray array) {
    System.out.println("freeArray: " + array.memoryBlock().pageNumber);

    freePage(array.memoryBlock());
  }

  private void throwOom(final MemoryBlock page, final long required) {
    long got = 0;
    if (page != null) {
      got = page.size();
      taskMemoryManager.freePage(page, this);
    }
    taskMemoryManager.showMemoryUsage();
    throw SparkCoreErrors.outOfMemoryError(required, got);
  }

  public synchronized void freeMemory(long size) {
    taskMemoryManager.releaseExecutionMemory(size, this);
    used -= size;
  }

  /**
   * Returns the offset in the page for the given page plus base offset address. Note that this
   * method assumes that the page number is valid.
   */
  public synchronized long getOffsetInPage(long pagePlusOffsetAddress) {
    return taskMemoryManager.getOffsetInPage(pagePlusOffsetAddress);
  }

  public synchronized long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    return TaskMemoryManager.encodePageNumberAndOffset(pageNumber, offsetInPage);
  }

  public synchronized long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage - page.getBaseOffset());
  }
}
