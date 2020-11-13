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
import hydra.core.transport.ValidationStrategy
import hydra.ingest.IngestorInfo
import hydra.ingest.services.IngestionFlowV2.V2IngestRequest
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import spray.json.JsObject

private object HydraIngestJsonSupport {
  private final case class IntermediateV2IngestRequest(
                                                        key: JsObject,
                                                        value: Option[JsObject],
                                                        validationStrategy: Option[ValidationStrategy],
                                                        useSimpleFormat: Option[Boolean]
                                                      )
}

trait HydraIngestJsonSupport extends HydraJsonSupport {

  import HydraIngestJsonSupport._
  import spray.json._

  private val publishResponseApply: (Int, Option[Long]) => PublishResponse = PublishResponse.apply
  implicit val publishResponseFormat: RootJsonFormat[PublishResponse] = jsonFormat2(publishResponseApply)

  implicit object ValidationStrategyFormat extends RootJsonFormat[ValidationStrategy] {

    def read(json: JsValue): ValidationStrategy = json match {
      case JsString(s) if s.toLowerCase == "strict" => ValidationStrategy.Strict
      case JsString(s) if s.toLowerCase == "relaxed" => ValidationStrategy.Relaxed
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[ValidationStrategy]
        val clazz = tpe.typeSymbol.asClass
        throw DeserializationException(
          s"expected a ValidationStrategy of ${clazz.knownDirectSubclasses}, but got $json"
        )
    }

    def write(obj: ValidationStrategy): JsValue = {
      JsString(obj.toString)
    }
  }

  private implicit val intermediateV2IngestRequestFormat: JsonFormat[IntermediateV2IngestRequest] =
    jsonFormat4(IntermediateV2IngestRequest)

  implicit object V2IngestRequestFormat extends RootJsonFormat[V2IngestRequest] {
    override def read(json: JsValue): V2IngestRequest = {
      val int = intermediateV2IngestRequestFormat.read(json)
      V2IngestRequest(int.key.compactPrint, int.value.map(_.compactPrint), int.validationStrategy, int.useSimpleFormat.getOrElse(true))
    }

    override def write(obj: V2IngestRequest): JsValue = {
      intermediateV2IngestRequestFormat.write(IntermediateV2IngestRequest(
        obj.keyPayload.parseJson.asJsObject, obj.valPayload.map(_.parseJson.asJsObject),
        obj.validationStrategy, Some(obj.useSimpleJsonFormat)))
    }
  }

  implicit val ingestorInfoFormat = jsonFormat4(IngestorInfo)

  implicit object IngestorStatusFormat extends RootJsonFormat[IngestorStatus] {

    override def write(obj: IngestorStatus): JsValue = {
      JsObject(
        Map(
          "code" -> JsNumber(obj.statusCode.intValue()),
          "message" -> JsString(obj.message)
        )
      )
    }

    override def read(json: JsValue): IngestorStatus = ???
  }

  implicit object IngestionReportFormat
      extends RootJsonFormat[IngestionReport] {

    def writeState[T <: IngestorStatus: JsonWriter](t: T) = t.toJson

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
