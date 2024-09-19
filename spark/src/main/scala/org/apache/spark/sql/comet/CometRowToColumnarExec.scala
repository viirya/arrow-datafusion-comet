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

package org.apache.spark.sql.comet

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.{CometRowIterator, Native}
import org.apache.comet.CometConf.COMET_BATCH_SIZE
import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.vector.NativeUtil

/**
 * Comet physical plan node for Spark `RowToColumnarExec`.
 *
 * This is used to execute a `RowToColumnarExec` physical operator by using Comet native engine.
 *
 * Note that this requires Spark using off-heap memory mode because it will pass memory addresses
 * of the input rows to the native engine.
 */
case class CometRowToColumnarExec(override val output: Seq[Attribute], child: SparkPlan)
    extends CometPlan
    with RowToColumnarTransition {

  override def nodeName: String = "CometRowToColumnar"

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar: Boolean = true

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(output, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometRowToColumnarExec =>
        this.output == other.output &&
        this.child == other.child
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, child)

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (!sparkContext.getConf.get(MEMORY_OFFHEAP_ENABLED)) {
      throw new IllegalStateException(
        "CometRowToColumnarExec requires Spark to use off-heap memory mode.")
    }

    // scalastyle:off println
    println("CometRowToColumnarExec: doExecuteColumnar")
    println("off-heap enabled: " + sparkContext.getConf.get(MEMORY_OFFHEAP_ENABLED))
    val schema = QueryPlanSerde.serializeSchema(StructType.fromAttributes(output))
    child
      .execute()
      .mapPartitionsInternal(iter => {
        val rowIter = iter.asInstanceOf[Iterator[UnsafeRow]]

        val nativeUtil = new NativeUtil()
        val nativeLib = new Native()
        val batch_size = String.valueOf(COMET_BATCH_SIZE.get()).toInt

        new Iterator[ColumnarBatch] {
          private var batch: Option[ColumnarBatch] = None

          def getRowInfo(): (Array[Long], Array[Int]) = {
            val rowAddrs = new ArrayBuffer[Long](batch_size)
            val rowSizes = new ArrayBuffer[Int](batch_size)

            var count = 0
            rowIter.foreach { row =>
              // scalastyle:off println
              println("row: " + row.getBaseObject)

              val addr = row.getBaseOffset()
              val size = row.getSizeInBytes()
              rowAddrs += addr
              rowSizes += size

              count += 1
              if (count >= batch_size) {
                return (rowAddrs.toArray, rowSizes.toArray)
              }
            }

            (rowAddrs.toArray, rowSizes.toArray)
          }

          override def hasNext: Boolean = {
            if (batch.isDefined) {
              return true
            }

            if (!iter.hasNext) {
              return false
            }

            val (rowAddrs, rowSizes) = getRowInfo()

            batch = nativeUtil.getNextBatch(
              output.length,
              (arrayAddrs, schemaAddrs) => {
                nativeLib.rowToColumnar(schema, rowAddrs, rowSizes, arrayAddrs, schemaAddrs)

                rowAddrs.length
              })

            batch.isDefined
          }

          override def next(): ColumnarBatch = {
            if (!hasNext) {
              throw new NoSuchElementException
            }

            val nextBatch = batch.get
            batch = None
            nextBatch
          }
        }
      })
  }
}
