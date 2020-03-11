package hydra.ingest.bootstrap

import java.lang.reflect.Modifier

import akka.actor.Props
import com.pluralsight.hydra.reflect.DoNotScan
import hydra.common.config.ConfigSupport
import hydra.common.reflect.ReflectionUtils
import hydra.core.bootstrap.{ReflectionsWrapper, ServiceProvider}

import scala.util.Try

object ActorFactory extends ConfigSupport {

  import ReflectionsWrapper._

  import scala.collection.JavaConverters._

  def getActors(): Seq[(String, Props)] = {
    val serviceProviders = scanFor(classOf[ServiceProvider])
    serviceProviders.flatMap { clz =>
      Try(ReflectionUtils.getObjectInstance(clz))
        .map(_.services)
        .getOrElse(clz.newInstance().services)
    }
  }

  private def scanFor[T](clazz: Class[T]): Seq[Class[_ <: T]] =
    reflections
      .getSubTypesOf(clazz)
      .asScala
      .filterNot(c => Modifier.isAbstract(c.getModifiers))
      .filterNot(c => c.isAnnotationPresent(classOf[DoNotScan]))
      .toSeq
}
