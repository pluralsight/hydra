package hydra.sql

/**
  * Created by alexsilva on 7/11/17.
  */
case class InvalidSchemaException(msg: String) extends RuntimeException(msg)

case class UnableToCreateException(msg: String) extends RuntimeException(msg)
