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

import akka.actor.Status.Failure
import akka.actor.{Props, _}
import akka.routing.{FromConfig, RoundRobinPool}
import com.typesafe.config.Config
import hydra.common.config.ActorConfigSupport
import hydra.common.util.ActorUtils
import hydra.core.ingest.{HydraRequest, Ingestor}
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry._
import org.joda.time.DateTime

import scala.collection.parallel.mutable.ParHashMap
import scala.util.Try

/**
  * Created by alexsilva on 1/12/16.
  */
class IngestorRegistry extends Actor with ActorLogging with ActorConfigSupport {

  private val ingestors: ParHashMap[String, RegisteredIngestor] = new ParHashMap()

  private object RegistrationLock

  override def receive: Receive = {
    case RegisterWithClass(group, clazz) =>
      val name = ActorUtils.actorName(clazz)
      val ingestor = doRegister(name, group, clazz)
      context.watch(ingestor.ref)
      sender ! IngestorInfo(name, group, ingestor.ref.path, ingestor.registrationTime)

    case Unregister(name) =>
      ingestors.remove(name) match {
        case Some(handler) =>
          context.unwatch(handler.ref)
          log.debug(s"Removed ingestor $name")
          context.stop(handler.ref)

        case None => log.info(s"Handler $name not found")
      }

    case FindByName(name) =>
      val result = ingestors.get(name).map(r => IngestorInfo(name, r.group, r.ref.path, r.registrationTime)).toSeq
      sender ! LookupResult(result)

    case FindAll =>
      val info = ingestors.values.map(r => IngestorInfo(r.name, r.group, r.ref.path, r.registrationTime))
      sender ! LookupResult(info.toList)

    case Terminated(handler) => {
      //todo: ADD RESTART
      log.error(s"Ingestor ${handler} terminated.")
    }
  }

  def doRegister(name: String, group:String, clazz: Class[_ <: Ingestor]): RegisteredIngestor = {
    RegistrationLock.synchronized {
      if (ingestors.contains(name))
        sender ! Failure(IngestorAlreadyRegisteredException(s"Ingestor $name is already registered."))
      val ingestor = RegisteredIngestor(name, group, context.actorOf(ingestorProps(name, clazz), name), DateTime.now())
      ingestors + (name -> ingestor)
      ingestor
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception => {
        //todo: Keep track of errors and start backing off handlers if too many errors
        SupervisorStrategy.Restart
      }
    }

  private def ingestorProps(name: String, ingestor: Class[_ <: Ingestor]): Props = {
    import configs.syntax._
    val path = self.path / name
    val routerPath = path.elements.drop(1).mkString("/", "/", "")
    val ip = Props(ingestor)
    rootConfig.get[Config](s"akka.actor.deployment.$routerPath")
      .map { cfg =>
        log.debug(s"Using router $routerPath for ingestor $name")
        FromConfig.props(ip)
      }
      .valueOrElse {
        log.debug(s"Using default round robin router for ingestor $name")
        Try(new RoundRobinPool(rootConfig.getConfig("akka.actor.deployment.default-ingestor-router"))).map(_.props(ip))
          .recover { case e: Exception =>
            log.error(e, s"Ingestor $name won't be routed. Unable to instantiate default router: ${e.getMessage}")
            ip
          }.get
      }
  }
}

case class RegisteredIngestor(name: String, group: String, ref: ActorRef, registrationTime: DateTime)

object IngestorRegistry {

  case class RegisterWithClass(group: String, clazz: Class[_ <: Ingestor])

  case class Unregister(name: String)

  case object FindAll

  case class FindByName(name: String)

  case class LookupResult(ingestors: Seq[IngestorInfo])

  case class FindByRequest(request: HydraRequest)

  case class LookupRequestResult(request: HydraRequest, ingestors: Seq[IngestorInfo])

  case class IngestorAlreadyRegisteredException(msg: String) extends RuntimeException(msg)

}