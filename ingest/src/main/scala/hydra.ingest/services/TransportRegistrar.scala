package hydra.ingest.services

import java.lang.reflect.Method

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.common.reflect.ReflectionUtils
import hydra.common.util.ActorUtils
import hydra.core.transport.{Transport, TransportSupervisor}
import hydra.ingest.bootstrap.ClasspathHydraComponentLoader
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.services.IngestorRegistry.Unregistered

import scala.util.Try

/**
  * This actor serves as an proxy between the handler registry
  * and the application.
  *
  * Created by alexsilva on 12/5/16.
  */
class TransportRegistrar extends Actor with ConfigSupport with LoggingAdapter {

  //TODO: Make this work like IngestorRegistrar

  private val pkgs = applicationConfig.get[List[String]]("transports.classpath-scan")
    .valueOrElse(Seq("hydra.transports"))

  private lazy val transports: Map[String, Class[_ <: Transport]] = {
    log.debug(s"Scanning package(s): [${pkgs.mkString}].")
    new ClasspathHydraComponentLoader(pkgs).transports.map(h => ActorUtils.actorName(h) -> h).toMap
  }

  override def receive = {
    case Unregistered(name) =>
      log.info(s"Ingestor $name was removed from the registry.")

    case IngestorInfo(name, group, path, _) =>
      log.info(s"Ingestor $name [$group] is available at $path")
  }

  override def preStart(): Unit = {
    bootstrap(transports)
  }

  private[services] def bootstrap(transports: Map[String, Class[_ <: Transport]]): Seq[Try[ActorRef]] = {
    transports.map { case (name, clazz) =>
      val maybeProps = Try(companion(clazz).map(c => c._2.invoke(c._1, applicationConfig).asInstanceOf[Props])
        .getOrElse(Props(clazz)))

      //todo: add to registry
      maybeProps.map { props =>
        val actor = context.actorOf(TransportSupervisor.props(name, props), name)
        println(actor.path)
        actor
      }.recover {
        case e: Exception => log.error(s"Unable to instantiate transport $name: ${e.getMessage}"); throw e
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
      companion.getClass.getMethods.toList.filter(m => m.getName == "props"
        && m.getParameterTypes.toList == List(classOf[Config])) match {
        case Nil => None
        case method :: Nil => Some(companion, method)
        case _ => None
      }
    } catch {
      case _: ClassNotFoundException => None
    }
  }
}
