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
trait ConfigSupport {

  private val defaultConfig = ConfigFactory.load()

  val applicationName: String = defaultConfig.getString("application.name")

  private val fileConfig = defaultConfig.get[String]("application.config.location")
    .map(f => ConfigFactory.parseFile(new java.io.File(f))).valueOrElse(ConfigFactory.empty())

  val rootConfig: Config = fileConfig.withFallback(defaultConfig)

  val applicationConfig: Config = rootConfig.getConfig(applicationName)

  def mergeConfig(addtlConfig: Option[Config]): Config = {
    addtlConfig match {
      case Some(conf) if !conf.isEmpty => conf.withFallback(rootConfig)
      case _ => rootConfig
    }
  }

  import scala.collection.JavaConverters._

  def toMap(cfg: ConfigObject): Map[String, Object] = {
    cfg.toConfig.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
  }

  def toMap(cfg: Config): Map[String, Object] = {
    cfg.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
  }
}