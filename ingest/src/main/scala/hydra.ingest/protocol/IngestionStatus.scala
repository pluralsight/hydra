/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.ingest.protocol

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.{HydraRequest, IngestorState}
import hydra.core.protocol._
import org.joda.time.DateTime

import scala.collection.mutable

/**
  * Created by alexsilva on 7/6/16.
  */
case class IngestionStatus(request: HydraRequest) {

  private val _ingestors: mutable.HashMap[String, IngestorState] = new mutable.HashMap

  def joined(ingestorName: String): Unit = {
    if (_ingestors.get(ingestorName).isDefined) {
      throw new IllegalArgumentException(s"Ingestor ${ingestorName} already joined.")
    }

    _ingestors.put(ingestorName, new IngestorState(DateTime.now(), status = IngestorJoined))
  }

  def summary: Seq[IngestorState] = _ingestors.values.toSeq

  def isComplete: Boolean = _ingestors.values.filterNot(_.status.completed).isEmpty

  def set(ingestorName: String, status: IngestorStatus): Unit = {
    val ingestor = _ingestors.getOrElse(ingestorName, new IngestorState(DateTime.now(), status = status))
      .copy(finishedAt = if (status.completed) Some(DateTime.now()) else None, status = status)
    _ingestors.update(ingestorName, ingestor)
  }

  def ingestors: Map[String, IngestorState] = Map(_ingestors.toSeq: _*)

  /**
    * Called by the ingestion supervisor if the ingestion timed out.
    * It will set all the non-completed ingestor's statuses to TimedOut
    */
  def timeOut(): Unit = {
    _ingestors.filter(_._2.status != IngestorCompleted)
      .foreach(i => _ingestors.update(i._1, i._2.copy(status = IngestorTimeout)))
  }

  def ingestorsInError: Seq[IngestorState] =
    _ingestors.filter(_._2.status != IngestorCompleted).values.toSeq

  def hasErrors = !ingestorsInError.isEmpty

  /**
    * If any ingestor reports an error, that becomes the status.
    *
    * @return
    */
  val ingestionStatus: StatusCode = if (hasErrors) ingestorsInError.head.status.statusCode else StatusCodes.OK
}