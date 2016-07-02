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
import scala.concurrent.{Await, ExecutionContext}

import io.gearpump.Message
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.TaskContext

// TODO: Analyse query, compute token ranges, automatically convert types, batch, ...
class CassandraSink[T: BoundStatementBuilder] private[cassandra] (
    connector: CassandraConnector,
    conf: WriteConf,
    query: String
  )(implicit ec: ExecutionContext)
  extends DataSink
  with Logging {

  private[this] val session = connector.openSession()
  private[this] var writer: TableWriter[T] = _

  def open(context: TaskContext): Unit = {
    val writerFuture = ListenableFutureUtil.toScalaFuture(session.prepareAsync(query))
      .map(new TableWriter[T](connector, _, conf))

    writer = Await.result(writerFuture, 10.seconds)
  }

  def write(message: Message): Unit = {
    writer.write(message.msg.asInstanceOf[T])
  }

  def close(): Unit = {
    connector.close(session)
  }
}
