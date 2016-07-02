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
import com.datastax.driver.core.Row
import io.gearpump.TimeStamp
import io.gearpump.experiments.cassandra.lib.{ReadConf, BoundStatementBuilder}
import io.gearpump.streaming.task.TaskContext
import org.mockito.Mockito._

class CassandraSourceEmbeddedSpec extends CassandraEmbeddedSpecBase {

  private def storeTestData(partitions: Int, rows: Int) = {
    val session = connector.openSession()

    (0 to partitions).map { partition =>
      (0 to rows).map { row =>
        session.execute(
          s"""INSERT INTO $tableWithKeyspace(
             |  partitioning_key, clustering_key, data)
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

    implicit val builder: BoundStatementBuilder[TimeStamp] = new BoundStatementBuilder[TimeStamp] {
      override def bind(value: TimeStamp): Seq[Object] = Seq("5", Int.box(5))
    }

    val source = new CassandraSource(
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
    assert(result.head.msg.asInstanceOf[Row].getString("data") == "data")

    val result2 = source.read(10)

    assert(result2.size == 10)
    assert(result2.head.msg.asInstanceOf[Row].getString("data") == "data")

    source.open(taskContext, Some(1L))

    val result3 = source.read(10)

    assert(result3.size == 1)
    assert(result3.head.msg.asInstanceOf[Row].getString("partitioning_key") == "5")
    assert(result3.head.msg.asInstanceOf[Row].getInt("clustering_key") == 5)
  }
}
