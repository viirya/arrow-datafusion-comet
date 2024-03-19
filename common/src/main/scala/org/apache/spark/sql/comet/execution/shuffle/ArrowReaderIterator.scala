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

class ArrowReaderIterator(channel: ReadableByteChannel) extends Iterator[ColumnarBatch] {

  private val reader = StreamReader(channel)
  private var batch = nextBatch()
  private var currentBatch: ColumnarBatch = null

  override def hasNext: Boolean = {
    if (batch.isDefined) {
      return true
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

    // Release the previous batch.
    // If it is not released, when closing the reader, arrow library will complain about
    // memory leak.
    if (currentBatch != null) {
      currentBatch.close()
      // scalastyle:off println
      println("closed previous reader's currentBatch")
    }

    // scalastyle:off println
    for (i <- 0 until nextBatch.numCols) {
      val col = nextBatch.column(i)
      col match {
        case c: CometPlainVector =>
          println(
            s"reader's CometBatchRDD column $i: ${c.numValues()}, ${c.dataType()}, " +
              s"${c.getValueVector()}")
        case _ =>
      }
    }

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
