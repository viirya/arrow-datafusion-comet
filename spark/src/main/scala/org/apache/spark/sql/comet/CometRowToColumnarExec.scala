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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import com.google.common.base.Objects
import org.apache.comet.CometConf.COMET_BATCH_SIZE
import org.apache.comet.vector.NativeUtil
import org.apache.comet.{CometRowIterator, Native}

/**
 * Comet physical plan node for Spark `RowToColumnarExec`.
 *
 * This is used to execute a `RowToColumnarExec` physical operator by using Comet native engine.
 */
case class CometRowToColumnarExec(override val output: Seq[Attribute], child: SparkPlan)
    extends CometPlan
    with RowToColumnarTransition {

  override def nodeName: String = "CometRowToColumnarExec"

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
        this.child == other.child &&
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, child)

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.execute().mapPartitionsInternal(iter => {
      val nativeUtil = new NativeUtil()
      val nativeLib = new Native()
      val batch_size = String.valueOf(COMET_BATCH_SIZE.get())

      new Iterator[ColumnarBatch] {
        private val batch = None

        override def hasNext: Boolean = {
          if (batch.isDefined) {
            return true
          }

          nativeUtil.getNextBatch(
            output.length,
            (arrayAddrs, schemaAddrs) => {
              nativeLib.rowToColumnar(new CometRowIterator(iter.asInstanceOf[Iterator[UnsafeRow]]), arrayAddrs, schemaAddrs)
            })
        }

        override def next(): A = {
          nativeUtil.getNextBatch(
            output.length,
            (arrayAddrs, schemaAddrs) => {
              nativeLib.rowToColumnar(new CometRowIterator(iter.asInstanceOf[Iterator[UnsafeRow]]), arrayAddrs, schemaAddrs)
            })
        }
      }
    })
  }
}


