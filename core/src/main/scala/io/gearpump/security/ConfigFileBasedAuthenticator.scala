/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.security

import io.gearpump.security.ConfigFileBasedAuthenticator._
import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}

object ConfigFileBasedAuthenticator {

  val ROOT = "gearpump.security.config-file-based-authenticator"
  val ADMINS = ROOT + "." + "administrators"
  val GUESTS = ROOT + "." + "guests"

  class Result(override val authenticated: Boolean, override val isAdministrator: Boolean)
    extends AuthenticationResult

  case class Credentials(admins: Map[String, String], guests: Map[String, String]) {
    def verify(user: String, password: String): Result = {
      if (admins.contains(user)) {
        new Result(verify(user, password, admins), isAdministrator = true)
      } else if (guests.contains(user)) {
        new Result(verify(user, password, guests), isAdministrator = false)
      } else {
        new Result(authenticated = false, isAdministrator = false)
      }
    }

    private def verify(user: String, password: String, map: Map[String, String]): Boolean = {
      val storedPass = map(user)
      PasswordUtil.verify(password, storedPass)
    }
  }
}


/**
 * See conf/gear.conf to find configuration entries related with this Authenticator.
 */
class ConfigFileBasedAuthenticator(config: Config) extends Authenticator {

  private val credentials = loadCredentials(config)

  override def authenticate(user: String, password: String, ec: ExecutionContext): Future[AuthenticationResult] = {
    implicit val ctx = ec
    Future {
      credentials.verify(user, password)
    }
  }

  private def loadCredentials(config: Config): Credentials = {
    val admins  = configToMap(config, ADMINS)
    val guests = configToMap(config, GUESTS)
    new Credentials(admins, guests)
  }

  private def configToMap(config : Config, path: String) = {
    import scala.collection.JavaConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) => k -> v.toString }
  }
}