package hydra.core.transport

/**
  * Defines an Ack Strategy for messages being sent by a producer.
  *
  * Ingestors producing records with an explicit ack strategy should be notified when
  * the record is produced (or not.)
  *
  * Created by alexsilva on 10/4/16.
  */

sealed trait AckStrategy

object AckStrategy {

  def apply(strategy: String): AckStrategy = {
    Option(strategy).map(_.trim.toLowerCase) match {
      case Some("explicit") => Explicit
      case _ => None
    }
  }

  case object Explicit extends AckStrategy

  case object None extends AckStrategy

}

