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

package org.apache.spark.sql.comet.execution.shuffle

import java.nio.channels.ReadableByteChannel

import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector._

class ArrowReaderIterator(channel: ReadableByteChannel, source: String)
    extends Iterator[ColumnarBatch] {

  private val reader = StreamReader(channel, source)
  private var batch = nextBatch()
  private var currentBatch: ColumnarBatch = null

  override def hasNext: Boolean = {
    if (batch.isDefined) {
      return true
    }

    // Release the previous batch.
    // If it is not released, when closing the reader, arrow library will complain about
    // memory leak.
    if (currentBatch != null) {
      // scalastyle:off println
      val usedMem = reader.getAllocator().getAllocatedMemory
      println("Closing batch: " + usedMem)

      var string = ""
      for (i <- 0 until currentBatch.numCols()) {
        currentBatch.column(i) match {
          case a: CometPlainVector =>
            val valueVector = a.getValueVector

            string += s"valueVector: $valueVector (${valueVector.getClass.getName})" + "\n"

            val buffers = valueVector.getBuffers(true)
            buffers.foreach { buffer =>
              println(s"freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
              // buffer.getReferenceManager.release(buffer.getReferenceManager.getRefCount)
              while (buffer.getReferenceManager.getRefCount > 0) {
                buffer.close()
              }
              println(s"after freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
            }

          case a: CometDictionaryVector =>
            val indices = a.indices
            val dictionary = a.values
            string += s"indices: ${indices.getValueVector}, " +
              s"(${indices.getValueVector.getClass.getName})" + "\n"
            string += s"dictionary: ${dictionary.getValueVector}" +
              s"(${dictionary.getValueVector.getClass.getName})" + "\n"
            string += s"dictionary dictId: ${indices.getValueVector.getField.getDictionary.getId}" +
              "\n"

            val buffers = indices.getValueVector.getBuffers(true)
            println(s"buffers: ${buffers.size}")

            buffers.foreach { buffer =>
              println(s"freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
              // buffer.getReferenceManager.release(buffer.getReferenceManager.getRefCount)
              while (buffer.getReferenceManager.getRefCount > 0) {
                buffer.close()
              }
              println(s"after freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
            }

            println(s"dictionary: ${dictionary.getValueVector}")

            val dictBuffers = dictionary.getValueVector.getBuffers(true)
            println(s"dictBuffers: ${dictBuffers.size}")

            dictBuffers.foreach { buffer =>
              println(s"freeing dict buffer $i: ${buffer.getReferenceManager.getRefCount}")
              // buffer.getReferenceManager.release(buffer.getReferenceManager.getRefCount)
              while (buffer.getReferenceManager.getRefCount > 0) {
                buffer.close()
              }
              println(s"after dict freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
            }

          case _ =>
        }
      }

      currentBatch.close()
      val freedMem = reader.getAllocator().getAllocatedMemory - usedMem
      println("After closing: " + reader.getAllocator().getAllocatedMemory)
      if (freedMem == 0) {
        println("No memory freed:" + "\n" + string)

        println(s"columns: ${currentBatch.numCols()}")
        for (i <- 0 until currentBatch.numCols()) {
          currentBatch.column(i) match {
            case a: CometPlainVector =>
              val valueVector = a.getValueVector
              println(s"valueVector: $valueVector")

              val buffers = valueVector.getBuffers(true)
              println(s"buffers: ${buffers.size}")
              buffers.foreach { buffer =>
                println(s"freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
                buffer.close()
                println(s"after freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
              }

              valueVector.close()

            case a: CometDictionaryVector =>
              val indices = a.indices
              val dictionary = a.values

              println(s"indices: ${indices.getValueVector}")

              val buffers = indices.getValueVector.getBuffers(true)
              println(s"buffers: ${buffers.size}")

              buffers.foreach { buffer =>
                println(s"freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
                buffer.close()
                println(s"after freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
              }

              println(s"dictionary: ${dictionary.getValueVector}")

              val dictBuffers = dictionary.getValueVector.getBuffers(true)
              println(s"dictBuffers: ${dictBuffers.size}")

              dictBuffers.foreach { buffer =>
                println(s"freeing dict buffer $i: ${buffer.getReferenceManager.getRefCount}")
                buffer.close()
                println(
                  s"after dict freeing buffer $i: ${buffer.getReferenceManager.getRefCount}")
              }

              indices.getValueVector.close()
              dictionary.getValueVector.close()
            case other =>
              println(s"other: ${other.getClass.getName}")
          }
        }

        println("After manual closing: " + reader.getAllocator().getAllocatedMemory)
      }
    }

    batch = nextBatch()
    if (batch.isEmpty) {
      return false
    }
    true
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    val nextBatch = batch.get

    currentBatch = nextBatch
    batch = None
    currentBatch
  }

  private def nextBatch(): Option[ColumnarBatch] = {
    reader.nextBatch()
  }

  def close(): Unit =
    synchronized {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      reader.close()
    }
}
