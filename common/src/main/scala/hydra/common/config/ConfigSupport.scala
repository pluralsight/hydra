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

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import configs.syntax._

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

  val externalConfig = loadExternalConfig(defaultConfig)

  val applicationName: String = defaultConfig.getString("application.name")

  val rootConfig: Config = externalConfig.withFallback(defaultConfig)

  val applicationConfig: Config = rootConfig.getConfig(applicationName)

  def loadExternalConfig(c: Config): Config = {
    c.getOrElse[String]("application.config.location", s"/etc/hydra/$applicationName.conf")
      .map(f => ConfigFactory.parseFile(new java.io.File(f)))
      .valueOrThrow(err => err.configException)
  }
}

object ConfigSupport {

  import scala.collection.JavaConverters._

  implicit def toMap(cfg: ConfigObject): Map[String, Object] = {
    cfg.toConfig.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
  }

  implicit def toMap(cfg: Config): Map[String, Object] = {
    cfg.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
  }

  implicit def toProps(map: Map[String, AnyRef]): Properties = {
    (map.foldLeft(new Properties)) {
      case (a, (k, v)) =>
        a.put(k, v)
        a
    }
  }
}