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

package org.apache.comet.shims

import org.apache.spark.sql.comet.execution.shuffle.{CometShuffleExchangeExec, ShuffleType}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

trait ShimCometShuffleExchangeExec {
  // TODO: remove after dropping Spark 3.2 and 3.3 support
  def apply(s: ShuffleExchangeExec, shuffleType: ShuffleType): CometShuffleExchangeExec = {
    val advisoryPartitionSize = s.getClass.getDeclaredMethods
      .filter(_.getName == "advisoryPartitionSize")
      .flatMap(_.invoke(s).asInstanceOf[Option[Long]])
      .headOption
    val cometShuffle = CometShuffleExchangeExec(
      s.outputPartitioning,
      s.child,
      s,
      s.shuffleOrigin,
      shuffleType,
      advisoryPartitionSize)
    // Set logical link to the new CometShuffleExchangeExec to make AQE work correctly
    // s.logicalLink.foreach(cometShuffle.setLogicalLink)
    // scalastyle:off println
    // println(
    //  s"s: $s, s.logicalLink: ${s.logicalLink}, cometShuffle: $cometShuffle, " +
    //    s"${cometShuffle.logicalLink}")
    cometShuffle
  }
}
