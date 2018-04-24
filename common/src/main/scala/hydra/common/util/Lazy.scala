package hydra.common.util

import scala.language.implicitConversions

/**
  * Supports lazy initializations of objects.
  */
object Lazy {

  def lazily[A](f: => A): Lazy[A] = new Lazy(f)

  implicit def evalLazy[A](l: Lazy[A]): A = l()

}

class Lazy[A] private(f: => A) {

  private var option: Option[A] = None

  def apply(): A = option match {
    case Some(a) => a
    case None => val a = f; option = Some(a); a
  }

  def isEvaluated: Boolean = option.isDefined

}