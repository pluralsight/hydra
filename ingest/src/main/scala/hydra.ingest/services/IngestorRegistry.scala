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

package hydra.ingest.services

import akka.actor.{Props, _}
import akka.routing.{FromConfig, RoundRobinPool}
import hydra.common.config.ActorConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.ingest.{HydraRequest, Ingestor}
import hydra.core.protocol.HydraMessage
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestorRegistry._
import org.joda.time.DateTime

import scala.collection.parallel.mutable.ParHashMap
import scala.util.Try

/**
  * Created by alexsilva on 1/12/16.
  */
class IngestorRegistry extends Actor with ActorLogging with ActorConfigSupport {

  private val ingestors: ParHashMap[String, RegisteredIngestor] =
    new ParHashMap()

  private object RegistrationLock

  override def receive: Receive = {
    case RegisterWithClass(clazz, group, name) =>
      val ingestorName = name getOrElse ActorUtils.actorName(clazz)
      sender ! doRegister(ingestorName, group, clazz)

    case Unregister(name) =>
      sender ! unregister(name)

    case FindByName(name) =>
      val result = ingestors
        .get(name)
        .map(r => IngestorInfo(name, r.group, r.ref.path, r.registrationTime))
        .toSeq
      sender ! LookupResult(result)

    case FindAll =>
      val info = ingestors.values.map(r =>
        IngestorInfo(r.name, r.group, r.ref.path, r.registrationTime)
      )
      sender ! LookupResult(info.toList)

    case Terminated(handler) => {
      log.error(s"Ingestor ${handler} terminated.")
      context.system.eventStream
        .publish(IngestorTerminated(handler.path.toString))
    }
  }

  private def unregister(name: String): HydraMessage = {
    RegistrationLock.synchronized {
      ingestors.remove(name) match {
        case Some(handler) =>
          context.unwatch(handler.ref)
          log.debug(s"Removed ingestor $name")
          context.stop(handler.ref)
          Unregistered(name)

        case None => IngestorNotFound(name)
      }
    }
  }

  def doRegister(
      name: String,
      group: String,
      clazz: Class[_ <: Ingestor]
  ): HydraMessage = {
    RegistrationLock.synchronized {
      ingestors
        .get(name)
        .map(i =>
          IngestorAlreadyRegistered(
            s"Ingestor $name is already registered at ${i.registrationTime}."
          )
        )
        .getOrElse {
          val ingestor = RegisteredIngestor(
            name,
            group,
            context.actorOf(ingestorProps(name, clazz), name),
            DateTime.now()
          )
          ingestors + (name -> ingestor)
          context.watch(ingestor.ref)
          IngestorInfo(
            name,
            group,
            ingestor.ref.path,
            ingestor.registrationTime
          )
        }
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception => {
        //todo: Keep track of errors and start backing off handlers if too many errors
        SupervisorStrategy.Restart
      }
    }

  private def ingestorProps(
      name: String,
      ingestor: Class[_ <: Ingestor]
  ): Props = {
    import hydra.common.config.ConfigSupport._
    val path = self.path / name
    val routerPath = path.elements.drop(1).mkString("/", "/", "")
    val ip = Props(ingestor)
    rootConfig
      .getConfigOpt(s"akka.actor.deployment.$routerPath")
      .map { cfg =>
        log.debug(s"Using router $routerPath for ingestor $name")
        FromConfig.props(ip)
      }
      .getOrElse {
        log.debug(s"Using default round robin router for ingestor $name")
        Try(
          new RoundRobinPool(
            rootConfig
              .getConfig("akka.actor.deployment.default-ingestor-router")
          )
        ).map(_.props(ip))
          .recover {
            case e: Exception =>
              log.error(
                e,
                s"Ingestor $name won't be routed. Unable to instantiate default router: ${e.getMessage}"
              )
              ip
          }
          .get
      }
  }
}

case class RegisteredIngestor(
    name: String,
    group: String,
    ref: ActorRef,
    registrationTime: DateTime
)

object IngestorRegistry {

  /**
    *
    * @param group The group to which register this ingestor.
    * @param name  The name to use; if None ActorUtils.actorName(clazz) will be used.
    * @param clazz The Ingestor class.
    */
  case class RegisterWithClass(
      clazz: Class[_ <: Ingestor],
      group: String,
      name: Option[String] = None
  )

  case class Unregister(name: String)

  case object FindAll

  case class FindByName(name: String)

  case class LookupResult(ingestors: Seq[IngestorInfo])

  case class FindByRequest(request: HydraRequest)

  case class IngestorAlreadyRegistered(msg: String) extends HydraMessage

  case class IngestorNotFound(name: String) extends HydraMessage

  case class Unregistered(name: String) extends HydraMessage

  case class IngestorTerminated(path: String) extends HydraMessage

}
