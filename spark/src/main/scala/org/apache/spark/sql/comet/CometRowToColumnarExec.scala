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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet physical plan node for Spark `RowToColumnarExec`.
 *
 * This is used to execute a `RowToColumnarExec` physical operator by using Comet native engine.
 */
case class CometRowToColumnarExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec
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
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, child)
}
