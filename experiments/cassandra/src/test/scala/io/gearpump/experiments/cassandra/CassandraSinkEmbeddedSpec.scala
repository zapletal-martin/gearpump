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

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

import io.gearpump.Message
import io.gearpump.streaming.task.TaskContext
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

class CassandraSinkEmbeddedSpec extends CassandraEmbeddedSpecBase {

  private[this] val insertStatement =
    s"""INSERT INTO $tableWithKeyspace(
        |  partitioning_key, clustering_key, data)
        |VALUES(?, ?, ?)
      """.stripMargin

  private def selectAll() = {
    val session = connector.openSession()
    session.execute(selectAllStatement)
  }

  override def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(10000L)
    createTables()
  }

  override def afterAll(): Unit = {
    connector.evictCache()
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  "CassandraSink" should "write data to Cassandra" in {
    implicit val builder: BoundStatementBuilder[(String, Int, String)] =
      new BoundStatementBuilder[(String, Int, String)] {
        override def bind(value: (String, Int, String)): Seq[Object] =
          Seq(value._1, Int.box(value._2), value._3)
      }

    val sink = new CassandraSink(
      connector,
      WriteConf(),
      insertStatement)

    val taskContext = mock[TaskContext]
    sink.open(taskContext)

    val message = Message(("1", 1, "data"))
    sink.write(message)

    val data = selectAll().all().asScala
    assert(data.size == 1)
    val first = data.head
    assert(first.getString("partitioning_key") == "1")
    assert(first.getInt("clustering_key") == 1)
    assert(first.getString("data") == "data")

    val message2 = Message(("1", 2, "data"))
    sink.write(message2)

    val data2 = selectAll().all().asScala
    assert(data2.size == 2)
    val last = data2.last
    assert(last.getString("partitioning_key") == "1")
    assert(last.getInt("clustering_key") == 2)
    assert(last.getString("data") == "data")
  }
}
