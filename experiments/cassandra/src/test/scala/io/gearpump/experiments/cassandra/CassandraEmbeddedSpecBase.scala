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

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

trait CassandraEmbeddedSpecBase
  extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  protected val conf = CassandraConnectorConf(
    port = 9142,
    hosts = Set(InetAddress.getByName("127.0.0.1")))
  protected val connector = new CassandraConnector(conf)

  protected val keyspace = "demo"
  protected val table = "CassandraSourceEmbeddedSpec"
  protected val tableWithKeyspace = s"$keyspace.$table"

  protected def createTables() = {
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
  }

  protected val selectAllStatement = s"SELECT * FROM $tableWithKeyspace"

  override def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(10000L)
    createTables()
  }

  override def afterAll(): Unit = {
    connector.evictCache()
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
}
