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

package hydra.core.extensions

import akka.actor._
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Hydra modules are a custom way to add functionality to Hydra Core.
  *
  * Modules are managed by Akka extensions.
  *
  * Created by alexsilva on 12/17/15.
  */
trait HydraModule extends LoggingAdapter {

  val context: ActorContext = TypedActor.context

  def config: Config

  @throws(classOf[Exception])
  def init(): Future[Boolean] = Future.successful(true)

  @throws(classOf[Exception])
  def run(): Unit

  @throws(classOf[Exception])
  def stop(): Unit = {}

}

case class HydraModuleWrapper(extension: String, id: String, interval: FiniteDuration,
                              initialDelay: FiniteDuration, ext: HydraModule) extends LoggingAdapter {
  def startModule(system: ActorSystem)(implicit ec: ExecutionContext) = {

    val start = System.currentTimeMillis
    ext.init.onComplete {
      case Success(started) => {
        if (started) {
          system.scheduler.schedule(initialDelay, interval)(ext.run)
          system.registerOnTermination(ext.stop())
          log.info(s"Initialized extension ${extension} in ${System.currentTimeMillis - start} ms")

        } else {
          log.error(s"Unable to start extension ${extension}. Init method return false. Not going to try again.")
          TypedActor(system).stop(ext)
        }
      }
      case Failure(ex) => {
        log.error(s"Unable to start extension ${extension}.", ex)
        TypedActor(system).stop(ext)
      }
    }
  }
}