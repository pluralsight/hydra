package hydra.core.ingest

import hydra.core.protocol.IngestorStatus
import org.joda.time.{DateTime, Seconds}

/**
  * Created by alexsilva on 3/1/17.
  */
case class IngestorState(startedAt: DateTime, finishedAt: Option[DateTime] = None, status: IngestorStatus) {
  /**
    * How long this Transport took to ingest a message, in milliseconds, or -1 if the ingestion
    * did not complete.
    *
    * @return
    */
  def duration: Long =
    finishedAt.map(t => Seconds.secondsBetween(t, startedAt).toStandardDuration.getMillis).getOrElse(-1L)

}
