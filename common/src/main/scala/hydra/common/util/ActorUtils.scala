package hydra.common.util

import akka.actor.{Actor, ActorRef}
import hydra.common.logging.LoggingAdapter

import scala.reflect.ClassTag

/**
  * Standardizes on actor names used across Hydra.
  *
  * Created by alexsilva on 2/17/17.
  */
object ActorUtils extends LoggingAdapter {

  /**
    * @see StringUtils.camel2underscores
    * @return The "standard" default name for actors used in Hydra, which is created by converting the
    *         class name of the actor from camel case to underscores.
    */
  def actorName[T: ClassTag]: String = {
    val clazzName = implicitly[ClassTag[T]].runtimeClass.getSimpleName
    StringUtils.camel2underscores(clazzName)
  }

  /**
    * @see StringUtils.camel2underscores
    * @return The "standard" default name for actors used in Hydra, which is created by converting the
    *         class name of the actor from camel case to underscores.
    */
  def actorName(clazz: Class[_ <: Actor]): String = {
    StringUtils.camel2underscores(clazz.getSimpleName)
  }

  def actorName(ref: ActorRef): String = {
    val elems = ref.path.elements.toList
    if (elems.last.startsWith("$")) elems.takeRight(2)(0) else elems.last
  }
}
