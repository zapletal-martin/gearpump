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

import java.net.InetAddress

import akka.actor.ActorSystem
import com.datastax.driver.core.Row
import io.gearpump.streaming.task.TaskContext
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CassandraSourceEmbeddedSpec
  extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  val conf = CassandraConnectorConf(
    port = 9142,
    hosts = Set(InetAddress.getByName("127.0.0.1")))
  val connector = new CassandraConnector(conf)

  val keyspace = "demo"
  val table = "CassandraSourceEmbeddedSpec"
  val tableWithKeyspace = s"$keyspace.$table"

  private def storeTestData(partitions: Int, rows: Int) = {
    val session = connector.openSession()

    session.execute(
      s"""CREATE KEYSPACE $keyspace
        |  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
      """.stripMargin)

    session.execute(
      s"""CREATE TABLE $tableWithKeyspace(
        |  partitioning_key text,
        |  clustering_key int,
        |  data text,
        |  PRIMARY KEY(partitioning_key, clustering_key)
        |)
      """.stripMargin)

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
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(10000L)
    storeTestData(10, 10)
  }

  override def afterAll(): Unit = {
    connector.evictCache()
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  "CassandraSource" should "read data from Cassandra" in {
    val query = s"SELECT * FROM $tableWithKeyspace"
    val queryWithWhere =
      s"""
        |SELECT *
        |FROM $tableWithKeyspace
        |WHERE partitioning_key = ? AND clustering_key = ?
      """.stripMargin

    val source = new CassandraSource(
      connector,
      ReadConf(),
      query,
      queryWithWhere,
      _ => Seq("5", Int.box(5)))

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
