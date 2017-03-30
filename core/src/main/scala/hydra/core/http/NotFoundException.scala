package hydra.core.http

/**
  * Created by alexsilva on 3/30/17.
  */
case class NotFoundException(msg: String) extends RuntimeException(msg)
