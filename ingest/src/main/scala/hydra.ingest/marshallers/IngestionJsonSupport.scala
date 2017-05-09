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

package hydra.ingest.marshallers

import hydra.core.ingest.RequestParams
import hydra.core.marshallers.HydraJsonSupport
import hydra.core.protocol.IngestorStatus
import hydra.ingest.ingestors.IngestorInfo
import hydra.ingest.protocol.IngestionReport


/**
  * Created by alexsilva on 2/18/16.
  */
trait IngestionJsonSupport extends HydraJsonSupport {

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
      val isDetailed = obj.metadata.find(_.name == RequestParams.HYDRA_RESPONSE_FORMAT)
        .map(_.value.equalsIgnoreCase("detailed")).getOrElse(true)
      val ingestors = obj.ingestors
        .map(h => if (isDetailed) h._1 -> writeState(h._2) else h._1 -> JsNumber(h._2.statusCode.intValue()))

      val response = Map(
        "requestId" -> (if (isDetailed) Some(JsString(obj.correlationId)) else None),
        "ingestors" -> (if (ingestors.isEmpty) None else Some(JsObject(ingestors)))
      ).collect {
        case (key, Some(value)) => key -> value
      }

      JsObject(response)
    }

    override def read(json: JsValue): IngestionReport = ???

  }

}
