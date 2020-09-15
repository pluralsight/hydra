/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.common.config

import java.util.Properties
import java.util.concurrent.TimeUnit

import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
  * Merges several configuration resources from different locations. More specifically:
  *
  * <ol><li>An external configuration file named '/etc/hydra/application.conf'</li>
  * <li>The default configurations (application.conf and reference.conf.</li>
  * </ol>
  * Properties defined in the external file override the internal application.conf and reference.conf properties.
  *
  * These configuration properties are exposed under a single object named ``config``
  * Created by alexsilva on 10/28/15.
  */
trait ConfigSupport extends ConfigComponent {

  private val defaultConfig = ConfigFactory.load()

  val applicationName: String = defaultConfig.getString("application.name")

  val rootConfig: Config = defaultConfig

  val applicationConfig: Config = rootConfig.getConfig(applicationName)

}

object ConfigSupport {

  import scala.collection.JavaConverters._

  implicit def toMap(cfg: ConfigObject): Map[String, Object] = {
    cfg.toConfig
      .entrySet()
      .asScala
      .map({ entry => entry.getKey -> entry.getValue.unwrapped() })(
        collection.breakOut
      )
  }

  implicit def toMap(cfg: Config): Map[String, Object] = {
    cfg
      .entrySet()
      .asScala
      .map({ entry => entry.getKey -> entry.getValue.unwrapped() })(
        collection.breakOut
      )
  }

  implicit def toProps(map: Map[String, AnyRef]): Properties = {
    map.foldLeft(new Properties) {
      case (a, (k, v)) =>
        a.put(k, v)
        a
    }
  }

  implicit class ConfigImplicits(config: Config) {
    def getDurationOpt(path: String): Option[FiniteDuration] =
      getOptional(path, config.getDuration).map(d => FiniteDuration(d.toNanos, TimeUnit.NANOSECONDS))

    def getStringOpt(path: String): Option[String] =
      getOptional(path, config.getString)

    def getConfigOpt(path: String): Option[Config] =
      getOptional(path, config.getConfig)

    def getIntOpt(path: String): Option[Int] =
      getOptional(path, config.getInt)

    def getBooleanOpt(path: String): Option[Boolean] =
      getOptional(path, config.getBoolean)

    def getStringListOpt(path: String): Option[List[String]] =
      getOptional(path, config.getStringList).map(_.asScala.toList)

    private def getOptional[A](path: String, method: String => A): Option[A] = {
      if (config.hasPath(path)) {
        method(path).some
      } else {
        none
      }
    }
  }

}
