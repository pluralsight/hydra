package hydra.common.util

/**
  * Created by alexsilva on 4/6/17.
  */

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util._

object Retry {

  @tailrec
  def retry[T](n: Int, pause: Duration)(fn: => T): Try[T] = {
    Try {
      fn
    } match {
      case x: Success[T] => x
      case _ if n > 1 =>
        Thread.sleep(pause.toMillis)
        retry(n - 1, pause)(fn)
      case fn => fn
    }
  }
}
