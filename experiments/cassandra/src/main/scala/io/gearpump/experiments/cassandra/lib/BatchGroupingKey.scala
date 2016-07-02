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
package io.gearpump.experiments.cassandra.lib

sealed trait BatchGroupingKey

object BatchGroupingKey {

  case object None extends BatchGroupingKey

  case object ReplicaSet extends BatchGroupingKey

  case object Partition extends BatchGroupingKey

  def apply(name: String): BatchGroupingKey = name.toLowerCase match {
    case "none" => None
    case "replica_set" => ReplicaSet
    case "partition" => Partition
    case _ => throw new IllegalArgumentException(s"Invalid batch level: $name")
  }
}
