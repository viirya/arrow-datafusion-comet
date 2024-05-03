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

package org.apache.comet.vector

import java.nio.channels.ReadableByteChannel

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.MessageChannelReader
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A reader that consumes Arrow data from an input channel, and produces Comet batches.
 */
case class StreamReader(channel: ReadableByteChannel, source: String) extends AutoCloseable {
  private var allocator = new RootAllocator(Long.MaxValue)
    .newChildAllocator(s"${this.getClass.getSimpleName}/$source", 0, Long.MaxValue)
  private val channelReader = new MessageChannelReader(new ReadChannel(channel), allocator)
  private var arrowReader = new ArrowStreamReader(channelReader, allocator)
  private var root = arrowReader.getVectorSchemaRoot

  def getAllocator(): BufferAllocator = allocator

  def nextBatch(): Option[ColumnarBatch] = {
    // scalastyle:off println
    println("Reading next batch: " + allocator.getAllocatedMemory)
    if (arrowReader.loadNextBatch()) {
      println("After read: " + allocator.getAllocatedMemory)
      Some(rootAsBatch(root))
    } else {
      println("EOF: " + allocator.getAllocatedMemory)
      None
    }
  }

  private def rootAsBatch(root: VectorSchemaRoot): ColumnarBatch = {
    // scalastyle:off println
    println("Converting batch: " + allocator.getAllocatedMemory)
    val batch = NativeUtil.rootAsBatch(root, arrowReader)
    println("After converting: " + allocator.getAllocatedMemory)
    batch
  }

  override def close(): Unit = {
    if (root != null) {
      arrowReader.close()
      root.close()
      allocator.close()

      arrowReader = null
      root = null
      allocator = null
    }
  }
}
