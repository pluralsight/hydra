package hydra.common.util

import scala.util.{Failure, Try}

object MonadUtils {

  def booleanToOption[A](check:Boolean)(body:()=>Option[A]):Option[A] = if (check) body.apply else None
  def booleanToTry[A](check:Boolean)(body:()=>Try[A]):Try[A] = if (check) body.apply else Failure[A]

}
