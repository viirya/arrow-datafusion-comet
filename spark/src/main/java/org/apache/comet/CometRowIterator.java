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

package org.apache.comet;

import scala.collection.Iterator;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * An iterator that can be used to expose Spark `UnsafeRow` to native execution. It takes
 * an iterator of `UnsafeRow` and returns the address of next `UnsafeRow` in the iterator,
 * which can be used by native code to access the data of the row, once this iterator is
 * consumed by native code.
 */
public class CometRowIterator {
    final Iterator<UnsafeRow> input;

    CometRowIterator(Iterator<UnsafeRow> input) {
        this.input = input;
    }

    /**
     * Get the address of next `UnsafeRow`.
     *
     * @return the address of `UnsafeRow`. 0 if there is no more row.
     */
    public long next() {
        boolean hasNext = input.hasNext();

        if (!hasNext) {
            return 0;
        }

        UnsafeRow row = input.next();

        if (row.getBaseObject() != null) {
            throw new IllegalStateException("The row is not in off-heap memory.");
        }

        return row.getBaseOffset();
    }
}
