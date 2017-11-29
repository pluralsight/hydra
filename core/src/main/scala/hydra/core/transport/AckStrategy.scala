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
      case Some("transport") => TransportAck
      case Some("local") => LocalAck
      case _ => NoAck
    }
  }

  /**
    * Waits for an explicit acknowledgment from the underlying transport.
    */
  case object TransportAck extends AckStrategy

  /**
    * It is in the journal, but not necessarily acked by the underlying transport.
    */
  case object LocalAck extends AckStrategy

  case object NoAck extends AckStrategy

}

