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

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import com.datastax.driver.core.Statement
import io.gearpump.experiments.cassandra.lib._
import io.gearpump.streaming.task.TaskContext
import io.gearpump.streaming.transaction.api.TimeReplayableSource
import io.gearpump.{Message, TimeStamp}

// TODO: Analyse query, compute token ranges, automatically convert types, ...
class CassandraSource private[cassandra] (
    connector: CassandraConnector,
    conf: ReadConf,
    query: String,
    // TODO: Or make optional and only read and filter in code (@see KafkaSource)
    queryReplay: String
  )(implicit boundStatementBuilder: BoundStatementBuilder[TimeStamp],
    ec: ExecutionContext)
  extends TimeReplayableSource
  with Logging {

  private[this] val session = connector.openSession()
  private[this] var iterator: PrefetchingResultSetIterator = _

  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
    implicit val _ = context.system.dispatcher

    val resultSetFuture = startTime.fold[Future[Statement]] {
      ListenableFutureUtil.toScalaFuture(session.prepareAsync(query))
        .map(_.bind())
    } { st =>
      ListenableFutureUtil.toScalaFuture(session.prepareAsync(queryReplay))
        .map(_.bind(boundStatementBuilder.bind(st): _*))
    }
    .map(_.setConsistencyLevel(conf.consistencyLevel))
    .flatMap(statement => ListenableFutureUtil.toScalaFuture(session.executeAsync(statement)))

    // TODO: Figure out how to make the initial query non blocking
    val resultSet = Await.result(resultSetFuture, 10.seconds)
    iterator = new PrefetchingResultSetIterator(resultSet, conf.fetchSizeInRows)
  }

  override def close(): Unit = {
    connector.close(session)
  }

  // TODO: Extract timestamp?
  override def read(batchSize: Int): List[Message] = {
    iterator.take(batchSize).toList.map(Message(_))
  }
}
