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

import akka.actor.{ActorRef, ActorSystem, Props, TypedActor, TypedProps}
import akka.pattern.{Backoff, BackoffSupervisor}
import com.typesafe.config.{Config, ConfigObject}
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.ReflectionUtils
import hydra.core.extensions.HydraExtension.Run

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 2/15/17.
  */

abstract class HydraExtensionBase(extensionName: String, extConfig: Config)(implicit system: ActorSystem)
  extends LoggingAdapter {

  type HM = BaseHydraExtension

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
    val c = java.lang.Class.forName(clazz).asInstanceOf[Class[HM]]
    log.debug(s"Starting module $extensionName::$moduleId.")
    val module = instantiate(c, moduleId, cfg)
    log.debug(s"Started module $extensionName::$moduleId.")
    HydraExtensionRegistry(system).register(moduleId, module)
  }

  private def instantiate(c: Class[_ <: HM], moduleId: String, cfg: Config): Either[ActorRef, HydraTypedExtension] = {
    implicit val ec = getDispatcher(moduleId)

    val interval = cfg.get[FiniteDuration]("interval").value
    val initialDelay = cfg.get[FiniteDuration]("initialDelay").value

    if (classOf[HydraExtension].isAssignableFrom(c)) {
      log.debug(s"Instantiating Hydra extension $extensionName::$moduleId.")
      val props = backOff(Props(c, moduleId, cfg), s"${extensionName}_${moduleId}")
      val ref = system.actorOf(props, s"${extensionName}_${moduleId}_supervisor")
      system.scheduler.schedule(initialDelay, interval, ref, Run)
      Left(ref)
    }
    else {
      log.debug(s"Instantiating Hydra typed extension $extensionName::$moduleId.")
      val md = TypedActor(system).typedActorOf(
        TypedProps[HydraTypedExtension](
          classOf[HydraTypedExtension],
          ReflectionUtils.instantiateClass(c.asInstanceOf[Class[HydraTypedExtension]], List(moduleId, cfg))
        ), s"${extensionName}_${moduleId}")

      Right(startTypedModule(system, md, interval, initialDelay))
    }
  }

  private def backOff(moduleProps: Props, moduleName: String): Props = {
    import scala.concurrent.duration._
    BackoffSupervisor.props(
      Backoff.onStop(
        moduleProps,
        childName = moduleName,
        minBackoff = 1.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2))
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

  private def startTypedModule(system: ActorSystem, ext: HydraTypedExtension, interval: FiniteDuration,
                               initialDelay: FiniteDuration)(implicit ec: ExecutionContext): HydraTypedExtension = {

    val start = System.currentTimeMillis
    ext.init.onComplete {
      case Success(started) =>
        if (started) {
          system.scheduler.schedule(initialDelay, interval)(ext.run)
          system.registerOnTermination(ext.stop())
          log.info(s"Initialized extension ${ext.id} in ${System.currentTimeMillis - start} ms")

        } else {
          log.error(s"Unable to start extension ${ext.id}. Init method return false. Not going to try again.")
          TypedActor(system).stop(ext)
        }

      case Failure(ex) =>
        log.error(s"Unable to start extension ${ext.id}.", ex)
        TypedActor(system).stop(ext)
    }

    ext
  }
}