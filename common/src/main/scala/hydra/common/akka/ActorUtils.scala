package hydra.common.akka

import hydra.common.util.StringUtils

import scala.reflect.ClassTag

/**
  * Created by alexsilva on 2/17/17.
  */
object ActorUtils {
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
  def actorName(clazz: Class[_]): String = {
    StringUtils.camel2underscores(clazz.getSimpleName)
  }

}
