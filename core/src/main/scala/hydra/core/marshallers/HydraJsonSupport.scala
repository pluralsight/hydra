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

package hydra.core.marshallers

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import akka.actor.ActorPath
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCode
import hydra.common.util.Resource._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json.{JsString, _}

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 12/4/15.
  */
trait HydraJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object StatusCodeJsonFormat extends JsonFormat[StatusCode] {

    override def write(t: StatusCode): JsValue =
      JsObject(
        "code" -> JsNumber(t.intValue()),
        "message" -> JsString(t.reason())
      )

    override def read(json: JsValue): StatusCode = ???
  }

  implicit object ActorPathJsonFormat extends JsonFormat[ActorPath] {

    override def write(t: ActorPath): JsValue = JsString(t.toString)

    override def read(json: JsValue): ActorPath =
      ActorPath.fromString(json.convertTo[String])
  }

  implicit object ThrowableJsonFormat extends JsonFormat[Throwable] {

    /** Write a throwable as an object with 'message' and 'stackTrace' fields. */
    override def write(t: Throwable): JsValue = {

      using(new StringWriter) { stringWriter =>
        using(new PrintWriter(stringWriter)) { printWriter =>
          t.printStackTrace(printWriter)
          JsObject(
            "message" -> JsString(t.getMessage),
            "stackTrace" -> JsString(stringWriter.toString)
          )
        }
      }
    }

    override def read(json: JsValue): Throwable =
      throw new NotImplementedError("Not implemented.")
  }

  implicit val genericServiceResponseFormat = jsonFormat2(
    GenericServiceResponse
  )

  implicit object UUIDFormat extends RootJsonFormat[UUID] {

    def write(obj: UUID): JsValue = {
      JsString(obj.toString)
    }

    def read(json: JsValue): UUID = json match {
      case JsString(s) =>
        Try(UUID.fromString(s)).getOrElse(deserializationError(s))
      case _ =>
        deserializationError(s"'${json.toString()}' is not a valid UUID.")
    }
  }

  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
    val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

    def write(obj: DateTime): JsValue = {
      JsString(formatter.print(obj))
    }

    def read(json: JsValue): DateTime = json match {
      case JsString(s) => Try(formatter.parseDateTime(s)).getOrElse(error(s))
      case _           => error(json.toString())
    }

    def error(v: Any): DateTime = {
      deserializationError(
        s"""
           |'$v' is not a valid date value. Dates must be in format:
           |     * date-opt-time     = date-element ['T' [time-element] [offset]]
           |     * date-element      = std-date-element | ord-date-element | week-date-element
           |     * std-date-element  = yyyy ['-' MM ['-' dd]]
           |     * ord-date-element  = yyyy ['-' DDD]
           |     * week-date-element = xxxx '-W' ww ['-' e]
           |     * time-element      = HH [minute-element] | [fraction]
           |     * minute-element    = ':' mm [second-element] | [fraction]
           |     * second-element    = ':' ss [fraction]
           |     * offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
           |     * fraction          = ('.' | ',') digit+
        """.stripMargin
      )
    }
  }

  /** Writer for an Try[T] where T has an implicit JsonWriter[T] */
  implicit def tryWriter[R: JsonWriter]: RootJsonWriter[Try[R]] =
    new RootJsonWriter[Try[R]] {

      override def write(responseTry: Try[R]): JsValue = {
        responseTry match {
          case Success(r) => JsObject("success" -> r.toJson)
          case Failure(t) => JsObject("failure" -> t.toJson)
        }
      }
    }

  implicit object StreamTypeFormat extends RootJsonFormat[StreamType] {

    def read(json: JsValue): StreamType = json match {
      case JsString("Notification") => Notification
      case JsString("History")      => History
      case JsString("CurrentState") => CurrentState
      case JsString("Telemetry")    => Telemetry
      case _ => {
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[StreamType]
        val clazz = tpe.typeSymbol.asClass
        throw new DeserializationException(
          s"expected a streamType of ${clazz.knownDirectSubclasses}, but got $json"
        )
      }
    }

    def write(obj: StreamType): JsValue = {
      JsString(obj.toString)
    }
  }

  implicit val genericErrorFormat = jsonFormat2(GenericError)

  implicit val topicCreationMetadataFormat = jsonFormat8(TopicMetadataRequest)

  implicit val genericSchemaFormat = jsonFormat2(GenericSchema)

}

case class GenericError(status: Int, errorMessage: String)

case class TopicMetadataRequest(
    schema: JsObject,
    streamType: StreamType,
    derived: Boolean,
    deprecated: Option[Boolean],
    dataClassification: String,
    contact: String,
    additionalDocumentation: Option[String],
    notes: Option[String]
)

case class GenericSchema(name: String, namespace: String) {
  def subject = s"$namespace.$name"
}

sealed trait StreamType
case object Notification extends StreamType
case object CurrentState extends StreamType
case object History extends StreamType
case object Telemetry extends StreamType
