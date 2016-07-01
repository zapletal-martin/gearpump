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

import java.io.IOException

import scala.collection.Iterator

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.PreparedStatement

class TableWriter[T: BoundStatementBuilder] (
    connector: CassandraConnector,
    statement: PreparedStatement,
    writeConf: WriteConf) {

  /** Main entry point */
  def write(data: Iterator[T]) {
    val session = connector.openSession()
    val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel, None, None)

    val boundStmtBuilder = implicitly[BoundStatementBuilder[T]]
    val batchType = Type.UNLOGGED

    val batchStmtBuilder = new BatchStatementBuilder(batchType, writeConf.consistencyLevel)

    // TODO: Fix grouped
    val batches =
      data.map(d => statement
        .setConsistencyLevel(writeConf.consistencyLevel)
        .bind(boundStmtBuilder.bind(d): _*))
      .grouped(5)
      .map(batchStmtBuilder.maybeCreateBatch)

    for (stmtToWrite <- batches) {
      queryExecutor.executeAsync(stmtToWrite)
    }

    queryExecutor.waitForCurrentlyExecutingTasks()

    if (!queryExecutor.successful) {
      throw new IOException(s"Failed to write statements to $statement.")
    }
  }
}
