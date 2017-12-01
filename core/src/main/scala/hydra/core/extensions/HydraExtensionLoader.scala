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

import akka.actor.{ActorSystem, DynamicAccess, ExtensionId, ExtensionIdProvider, ReflectiveDynamicAccess}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import configs.syntax._
import hydra.common.logging.LoggingAdapter

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 2/15/17.
  */
object HydraExtensionLoader extends LoggingAdapter {

  def load(config: Config)(implicit system: ActorSystem): Seq[Try[ExtensionId[_]]] = {
    config.getOrElse[Config]("extensions", ConfigFactory.empty).map { c =>
      c.root().entrySet().asScala.map { entry =>
        val name = entry.getKey
        val cfg = entry.getValue.asInstanceOf[ConfigObject].toConfig
        //is there a way to plug into akka's classloader?
        val dm = new ReflectiveDynamicAccess(Thread.currentThread.getContextClassLoader)
        val clazz = cfg.getString("class")
        val enabled = cfg.get[Boolean]("enabled").valueOrElse(true)
        val extName = s"$name ($clazz)"
        if (enabled) {
          log.info(s"Loading extension ${extName}")
          instantiate(dm, system, clazz, name)
        } else {
          Failure(new IllegalArgumentException(s"Extension $name is not enabled."))
        }
      }
    }.value.toSeq
  }

  private def instantiate(dm: DynamicAccess, system: ActorSystem,
                          clazz: String, name: String): Try[ExtensionId[_]] = {
    val start = System.currentTimeMillis

    val ext = dm.getObjectFor[AnyRef](clazz).recoverWith {
      case _ => dm.createInstanceFor[AnyRef](clazz, Nil)
    }

    //register with akka
    val hydraExt = ext match {
      case Success(p: ExtensionIdProvider) =>
        system.registerExtension(p.lookup())
        Success(p.lookup())
      case Success(p: ExtensionId[_]) =>
        system.registerExtension(p)
        Success(p)
      case Success(x) =>
        throw new IllegalArgumentException(s"Unexpected result $x when trying to load extension [[$clazz]. Skipping.")
      case Failure(e) =>
        log.error(s"While trying to load extension [$clazz]: ${e.getMessage}. Skipping.")
        throw e
    }

    log.info(s"Finished loading extension ${name} in ${System.currentTimeMillis - start}ms.")

    hydraExt
  }
}