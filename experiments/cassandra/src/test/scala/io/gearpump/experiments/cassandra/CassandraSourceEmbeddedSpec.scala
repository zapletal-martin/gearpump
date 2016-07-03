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

import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.ActorSystem
import io.gearpump.TimeStamp
import io.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import io.gearpump.experiments.cassandra.lib.ReadConf
import io.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import io.gearpump.streaming.task.TaskContext
import org.mockito.Mockito._

class CassandraSourceEmbeddedSpec extends CassandraEmbeddedSpecBase {

  private def storeTestData(partitions: Int, rows: Int) = {
    val session = connector.openSession()

    (0 to partitions).map { partition =>
      (0 to rows).map { row =>
        session.execute(
          s"""
            |INSERT INTO $tableWithKeyspace(partitioning_key, clustering_key, data)
            |VALUES('$partition', $row, 'data')
          """.stripMargin)
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    storeTestData(10, 10)
  }

  "CassandraSource" should "read data from Cassandra" in {
    val queryWithWhere =
      s"""
        |SELECT *
        |FROM $tableWithKeyspace
        |WHERE partitioning_key = ? AND clustering_key = ?
      """.stripMargin

    case class Data(partitioningKey: String, clusterinKey: Int, data: String)

    implicit val builder: BoundStatementBuilder[TimeStamp] =
      value => Seq("5", Int.box(5))

    implicit val rowExtractor: RowExtractor[Data] = row =>
      Data(
        row.getString("partitioning_key"),
        row.getInt("clustering_key"),
        row.getString("data"))

    val source = new CassandraSource[Data](
      connector,
      ReadConf(),
      selectAllStatement,
      queryWithWhere)

    val actorSystem = ActorSystem("CassandraSourceEmbeddedSpec")
    val taskContext = mock[TaskContext]
    when(taskContext.system).thenReturn(actorSystem)

    source.open(taskContext, None)
    val result = source.read(10)

    assert(result.size == 10)
    assert(result.head.msg.asInstanceOf[Data].data == "data")

    val result2 = source.read(10)

    assert(result2.size == 10)
    assert(result2.head.msg.asInstanceOf[Data].data == "data")

    source.open(taskContext, Some(1L))

    val result3 = source.read(10)

    assert(result3.size == 1)
    assert(result3.head.msg.asInstanceOf[Data].partitioningKey == "5")
    assert(result3.head.msg.asInstanceOf[Data].clusterinKey == 5)
  }
}
