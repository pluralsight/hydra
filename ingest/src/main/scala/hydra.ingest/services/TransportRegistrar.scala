package hydra.ingest.services

import java.lang.reflect.Method

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.ReflectionUtils
import hydra.common.util.ActorUtils
import hydra.core.transport.Transport
import hydra.ingest.bootstrap.ClasspathHydraComponentLoader
import hydra.ingest.services.TransportRegistrar.{
  GetTransports,
  GetTransportsResponse
}

import scala.util.Try

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class TransportRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  //TODO: Make this work like IngestorRegistrar
  private lazy val transports: Map[String, Class[_ <: Transport]] =
    ClasspathHydraComponentLoader.transports
      .map(h => ActorUtils.actorName(h) -> h)
      .toMap

  override def receive = {
    case GetTransports => sender ! GetTransportsResponse(transports.keys.toSeq)
  }

  override def preStart(): Unit = {
    TransportRegistrar.bootstrap(transports, context, rootConfig)
  }
}

object TransportRegistrar extends LoggingAdapter {

  case object GetTransports

  case class GetTransportsResponse(transports: Seq[String])

  private[services] def bootstrap(
      transports: Map[String, Class[_ <: Transport]],
      fact: ActorRefFactory,
      config: Config
  ): Seq[Try[ActorRef]] = {
    transports.map {
      case (name, clazz) =>
        val maybeProps = Try(
          companion(clazz)
            .map(c => c._2.invoke(c._1, config).asInstanceOf[Props])
            .getOrElse(Props(clazz))
        )

        //todo: add to registry
        maybeProps
          .map { props =>
            log.debug(s"Initializing transport actor $name")
            val transport = fact.actorOf(props, name)
            log.debug(s"Initialized transport $name at ${transport.path}")
            transport
          }
          .recover {
            case e: Exception =>
              log.error(
                s"Unable to instantiate transport $name: ${e.getMessage}"
              )
              throw e
          }
    }.toSeq
  }

  /**
    * Looks for a companion object that has a method named "props" with a single argument of type Config.
    *
    * @param clazz The class to look for the companion object
    * @return
    */
  private def companion[T](clazz: Class[T]): Option[(T, Method)] = {
    try {
      val companion = ReflectionUtils.companionOf(clazz)
      companion.getClass.getMethods.toList.filter(m =>
        m.getName == "props"
          && m.getParameterTypes.toList == List(classOf[Config])
      ) match {
        case Nil           => None
        case method :: Nil => Some(companion, method)
        case _             => None
      }
    } catch {
      case _: Throwable => None
    }
  }
}
