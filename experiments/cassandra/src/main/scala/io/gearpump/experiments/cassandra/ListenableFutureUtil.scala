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

import java.util.concurrent.{ExecutionException, Executor}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.Try

import com.google.common.util.concurrent.ListenableFuture

object ListenableFutureUtil {

  implicit def toScalaFuture[A](
      listenableFuture: ListenableFuture[A])(
      implicit ec: ExecutionContext
    ): Future[A] = {

    val promise = Promise[A]

    listenableFuture.addListener(
      new Runnable {
        override def run(): Unit = promise.complete(Try(listenableFuture.get))
      },
      new Executor {
        override def execute(command: Runnable): Unit = ec.execute(command)
      }
    )

    // DESNOTE(2015.12.02, tkowalczyk): The exception coming out of Guava
    // future is wrapped in java.util.concurrent.ExecutionException

    promise.future.recover {
      case ex: ExecutionException =>
        throw ex.getCause
    }
  }
}
