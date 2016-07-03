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
package io.gearpump.streaming.examples.cassandra

import java.net.InetAddress
import java.util.Date

import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.ActorSystem
import io.gearpump.TimeStamp
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import io.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import io.gearpump.experiments.cassandra.lib._
import io.gearpump.experiments.cassandra.{CassandraSink, CassandraSource}
import io.gearpump.streaming.StreamApplication
import io.gearpump.streaming.sink.DataSinkProcessor
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.util.Graph._
import io.gearpump.util.{AkkaApp, Graph}

object CassandraTransform extends AkkaApp with ArgumentsParser {

  // CREATE KEYSPACE example
  //   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }

  // CREATE TABLE example.sensor_data(
  //   id string,
  //   inserted timestamp,
  //   temperature int,
  //   location string,
  //   PRIMARY KEY(id, inserted)
  // )

  // CREATE TABLE example.sensor_data_by_location(
  //   id string,
  //   inserted timestamp,
  //   temperature int,
  //   location string,
  //   PRIMARY KEY(location, inserted)
  // )

  // INSERT INTO example.sensor_data(id, inserted, temperature, location)
  // VALUES('1', '2016-07-01 15:00:00', 26, 'New York')

  // INSERT INTO example.sensor_data(id, inserted, temperature, location)
  // VALUES('2', '2016-07-01 15:00:00', 28, 'New York')

  // INSERT INTO example.sensor_data(id, inserted, temperature, location)
  // VALUES('2', '2016-07-02 15:00:00', 25, 'New York')

  // INSERT INTO example.sensor_data(id, inserted, temperature, location)
  // VALUES('3', '2016-07-01 15:00:00', 14, 'London')

  private[this] val query =
    """
      |SELECT * FROM example.sensor_data
    """.stripMargin

  private[this] val insert =
    """
      |INSERT INTO example.sensor_data_by_location(id, inserted, temperature, location)
      |VALUES(?, ?, ?, ?)
    """.stripMargin

  override val options: Array[(String, CLIOption[Any])] = Array(
    "host" -> CLIOption[String]("<cassandra host>", required = false,
      defaultValue = Some("127.0.0.1")),
    "port" -> CLIOption[Int]("<cassandra port>", required = false,
      defaultValue = Some(9042)),
    "source" -> CLIOption[Int]("<how many cassandra source tasks>", required = false,
      defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<how many cassandra sink tasks>", required = false,
      defaultValue = Some(1))
  )

  def application(config: ParseResult, system: ActorSystem): StreamApplication = {
    implicit val actorSystem = system
    val sourceNum = config.getInt("source")
    val sinkNum = config.getInt("sink")
    val cassandraHost = InetAddress.getByName(config.getString("host"))
    val cassandraPort = config.getInt("port")

    val appConfig = UserConfig.empty

    val connector = new CassandraConnector(
      CassandraConnectorConf(hosts = Set(cassandraHost), port = cassandraPort))

    case class SensorData(id: String, inserted: Date, temperature: Int, location: String)

    implicit val statementBuilder: BoundStatementBuilder[TimeStamp] = value => Seq()

    implicit val rowExtractor: RowExtractor[SensorData] = row =>
      SensorData(
        row.getString("id"),
        row.getTimestamp("inserted"),
        row.getInt("temperature"),
        row.getString("location"))

    val source = new CassandraSource[SensorData](connector, ReadConf(), query, query)
    val sourceProcessor = DataSourceProcessor(source, sourceNum)

    implicit val statementBuilder2: BoundStatementBuilder[SensorData] = value =>
      Seq(value.id, value.inserted, Int.box(value.temperature), value.location)

    val sink = new CassandraSink[SensorData](connector, WriteConf(), insert)
    val sinkProcessor = DataSinkProcessor(sink, sinkNum)

    val computation = sourceProcessor ~> sinkProcessor
    val app = StreamApplication("KafkaWordCount", Graph(computation), appConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config, context.system))
    context.close()
  }
}
