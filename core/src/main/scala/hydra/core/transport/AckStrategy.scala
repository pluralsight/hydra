package hydra.core.transport

import scala.util.Try

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

  def apply(strategy: String): Try[AckStrategy] = {
    Try {
      Option(strategy)
        .map(_.trim.toLowerCase)
        .collect {
          case "replicated"   => Replicated
          case "persisted"    => Persisted
          case "noack"        => NoAck
          case s if s.isEmpty => NoAck
          case x =>
            throw new IllegalArgumentException(
              s"$x is not a valid ack strategy."
            )
        }
        .getOrElse(NoAck)
    }
  }

  /**
    * Waits for an explicit acknowledgment from the underlying transport.
    */
  case object Replicated extends AckStrategy

  /**
    * It is in the journal, but not necessarily acked by the underlying transport.
    */
  case object Persisted extends AckStrategy

  case object NoAck extends AckStrategy

}
