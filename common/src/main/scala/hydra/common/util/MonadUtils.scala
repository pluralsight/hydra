package hydra.common.util

object MonadUtils {

  def booleanToOption[A](check: Boolean)(body: () => Option[A]): Option[A] =
    if (check) body.apply else None
}
