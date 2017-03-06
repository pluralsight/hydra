/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.core.extensions

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import com.typesafe.config.{Config, ConfigObject}
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.ReflectionUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Created by alexsilva on 2/15/17.
  */

abstract class HydraExtensionBase(extensionName: String, extConfig: Config)(implicit system: ActorSystem)
  extends LoggingAdapter {

  type HM = HydraModule

  extConfig.root.entrySet().asScala.foreach { entry =>
    val moduleId = entry.getKey
    try {
      val ecfg = entry.getValue.asInstanceOf[ConfigObject].toConfig
      val enabled = ecfg.get[Boolean]("enabled").valueOrElse(true)
      if (enabled) {
        startModule(moduleId, ecfg)
      } else {
        log.info(s"Module $extensionName::$moduleId is not enabled; it will not be started.")
      }
    } catch {
      case NonFatal(e: Throwable) => log.error("Unable to load module %s::%s. Reason: %s"
        .format(extensionName, moduleId, e.getMessage), e)
    }

  }

  private def startModule(moduleId: String, cfg: Config) = {
    val clazz = cfg.getString("class")
    val c = java.lang.Class.forName(clazz).asInstanceOf[Class[HydraModule]]
    val module = TypedActor(system).typedActorOf(
      TypedProps[HM](
        classOf[HM],
        ReflectionUtils.instantiateClassByName[HM](clazz, List(cfg))
      ), s"${extensionName}_${moduleId}"
    )

    val interval = cfg.get[FiniteDuration]("interval").value
    val initialDelay = cfg.get[FiniteDuration]("initialDelay").value
    register(moduleId, module, interval, initialDelay)
  }

  private def register(moduleId: String, module: HydraModule, interval: FiniteDuration, initDelay: FiniteDuration) = {
    implicit val ec = getDispatcher(moduleId)
    val wrapper = HydraModuleWrapper(extensionName, moduleId, interval, initDelay, module)
    log.debug(s"Registering module $extensionName::moduleId.")
    HydraModuleRegistry(system).addModule(wrapper)
    log.debug(s"Starting module $extensionName::moduleId.")
    wrapper.startModule(system)
    log.debug(s"Started module $extensionName::moduleId.")
  }

  private def getDispatcher(moduleId: String) = {
    val dispatcher = s"akka.actor.$extensionName.$moduleId"

    Try(system.dispatchers.lookup(dispatcher)).recover {
      case c: akka.ConfigurationException => {
        log.info(s"Module dispatcher $dispatcher not found. Using default dispatcher for $moduleId.")
        system.dispatchers.lookup(s"akka.actor.$extensionName.default")
      }
    }.get
  }
}