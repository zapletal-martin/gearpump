/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.experiments.cassandra

import com.datastax.driver.core.{Row, ResultSet}

/** Allows to efficiently iterate over a large, paged ResultSet,
 * asynchronously prefetching the next page.
 *
 * @param resultSet result set obtained from the Java driver
 * @param prefetchWindowSize if there are less than this rows available without blocking,
 *  initiates fetching the next page
 */
class PrefetchingResultSetIterator(
    resultSet: ResultSet,
    prefetchWindowSize: Int
  ) extends Iterator[Row] {

  private[this] val iterator = resultSet.iterator()

  override def hasNext: Boolean = iterator.hasNext

  private[this] def maybePrefetch(): Unit = {
    if (!resultSet.isFullyFetched && resultSet.getAvailableWithoutFetching < prefetchWindowSize) {
      resultSet.fetchMoreResults()
    }
  }

  override def next(): Row = {
    maybePrefetch()
    iterator.next()
  }
}
