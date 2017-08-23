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
import hydra.core.extensions.HydraActorModule.Run

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 2/15/17.
  */

trait HydraExtension {
  def extName: String

  def extConfig: Config
}

abstract class HydraExtensionBase(val extName: String, val extConfig: Config)(implicit system: ActorSystem)
  extends HydraExtension with LoggingAdapter {

  type HM = HydraModule

  extConfig.root.entrySet().asScala.foreach { entry =>
    val moduleId = entry.getKey
    try {
      val ecfg = entry.getValue.asInstanceOf[ConfigObject].toConfig
      val enabled = ecfg.get[Boolean]("enabled").valueOrElse(true)
      if (enabled) {
        startModule(moduleId, ecfg)
      } else {
        log.info(s"Module $extName::$moduleId is not enabled; it will not be started.")
      }
    } catch {
      case NonFatal(e: Throwable) => log.error("Unable to load module %s::%s. Reason: %s"
        .format(extName, moduleId, e.getMessage), e)
    }

  }

  private def startModule(moduleId: String, cfg: Config) = {
    val clazz = cfg.getString("class")
    val c = java.lang.Class.forName(clazz).asInstanceOf[Class[HM]]
    log.debug(s"Starting module $extName::$moduleId.")
    val module = instantiate(c, moduleId, cfg)
    log.debug(s"Started module $extName::$moduleId.")
    HydraExtensionRegistry(system).register(moduleId, module)
  }

  private def instantiate(c: Class[_ <: HM], moduleId: String, cfg: Config): Either[ActorRef, HydraTypedModule] = {
    implicit val ec = getDispatcher(moduleId)

    val interval = cfg.get[FiniteDuration]("interval").value
    val initialDelay = cfg.get[FiniteDuration]("initialDelay").value

    if (classOf[HydraModule].isAssignableFrom(c)) {
      log.debug(s"Instantiating Hydra extension $extName::$moduleId.")
      val props = backOff(Props(c, moduleId, cfg), s"${extName}_${moduleId}")
      val ref = system.actorOf(props, s"${extName}_${moduleId}_supervisor")
      system.scheduler.schedule(initialDelay, interval, ref, Run)
      Left(ref)
    }
    else {
      log.debug(s"Instantiating Hydra typed extension $extName::$moduleId.")
      val md = TypedActor(system).typedActorOf(
        TypedProps[HydraTypedModule](
          classOf[HydraTypedModule],
          ReflectionUtils.instantiateClass(c.asInstanceOf[Class[HydraTypedModule]], List(moduleId, cfg))
        ), s"${extName}_${moduleId}")

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
    val dispatcher = s"akka.actor.$extName.$moduleId"

    Try(system.dispatchers.lookup(dispatcher)).recover {
      case c: akka.ConfigurationException => {
        log.info(s"Module dispatcher $dispatcher not found. Using default dispatcher for $moduleId.")
        system.dispatchers.lookup(s"akka.actor.$extName.default")
      }
    }.get
  }

  private def startTypedModule(system: ActorSystem, ext: HydraTypedModule, interval: FiniteDuration,
                               initialDelay: FiniteDuration)(implicit ec: ExecutionContext): HydraTypedModule = {

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