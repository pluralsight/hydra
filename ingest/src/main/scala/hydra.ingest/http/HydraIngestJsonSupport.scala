/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.ingest.http

import hydra.core.ingest.IngestionReport
import hydra.core.marshallers.HydraJsonSupport
import hydra.core.protocol.IngestorStatus
import hydra.ingest.IngestorInfo


/**
  * Created by alexsilva on 2/18/16.
  */
trait HydraIngestJsonSupport extends HydraJsonSupport {

  import spray.json._


  implicit val ingestorInfoFormat = jsonFormat4(IngestorInfo)

  implicit object IngestorStatusFormat extends RootJsonFormat[IngestorStatus] {

    override def write(obj: IngestorStatus): JsValue = {
      JsObject(Map(
        "code" -> JsNumber(obj.statusCode.intValue()),
        "message" -> JsString(obj.message)
      )
      )
    }

    override def read(json: JsValue): IngestorStatus = ???
  }

  implicit object IngestionReportFormat extends RootJsonFormat[IngestionReport] {

    def writeState[T <: IngestorStatus : JsonWriter](t: T) = t.toJson

    override def write(obj: IngestionReport): JsValue = {

      val ingestors = obj.ingestors.map(h => h._1 -> writeState(h._2))
      val response = Map(
        "correlationId" -> JsString(obj.correlationId),
        "ingestors" -> JsObject(ingestors)
      )

      JsObject(response)
    }

    override def read(json: JsValue): IngestionReport = ???

  }

}
