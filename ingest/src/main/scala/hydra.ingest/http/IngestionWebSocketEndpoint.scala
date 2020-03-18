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

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.stream.StreamLimitReachedException
import akka.stream.scaladsl.{Flow, RestartFlow, Source}
import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.core.http.RouteSupport
import hydra.core.marshallers.GenericServiceResponse
import hydra.ingest.services.{
  IngestSocketFactory,
  IngestionOutgoingMessage,
  SimpleOutgoingMessage
}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.util.Failure

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionWebSocketEndpoint()(implicit system: ActorSystem)
    extends RouteSupport
    with HydraIngestJsonSupport {

  //visible for testing
  private[http] val enabled = applicationConfig
    .getBooleanOpt("ingest.websocket.enabled")
    .getOrElse(false)

  private val socketFactory = IngestSocketFactory.createSocket(system)

  implicit val simpleOutgoingMessageFormat: JsonFormat[SimpleOutgoingMessage] =
    jsonFormat2(SimpleOutgoingMessage)

  override val route: Route =
    path("ws-ingest") {
      if (enabled) {
        handleWebSocketMessages(ingestSocketFlow())
      } else {
        complete(
          StatusCodes.Conflict,
          GenericServiceResponse(409, "Web Socket not available.")
        )
      }
    }

  private[http] def ingestSocketFlow(): Flow[Message, Message, Any] = {
    RestartFlow.withBackoff(3.seconds, 30.seconds, 0.2, 20) { () =>
      Flow[Message]
        .flatMapConcat {
          case TextMessage.Strict(txt) => Source.single(txt)
          case TextMessage.Streamed(src) =>
            src
              .limit(IngestionWebSocketEndpoint.maxNumberOfWSFrames)
              .completionTimeout(
                IngestionWebSocketEndpoint.streamedWSMessageTimeout
              )
              .fold("")(_ + _)
          case _: BinaryMessage =>
            throw new IllegalArgumentException(
              "Binary messages are not supported."
            )
        }
        .via(socketFactory.ingestFlow())
        .recover {
          case s: StreamLimitReachedException =>
            SimpleOutgoingMessage(
              400,
              s"Frame limit reached after frame number ${s.n}."
            )
          case t: TimeoutException =>
            SimpleOutgoingMessage(400, s"Timeout on frame buffer reached.")
          case i: IllegalArgumentException =>
            SimpleOutgoingMessage(400, i.getMessage)
        }
        .map {
          case m: SimpleOutgoingMessage => TextMessage(m.toJson.compactPrint)
          case r: IngestionOutgoingMessage =>
            TextMessage(r.report.toJson.compactPrint)
        }
        .via(reportErrorsFlow)
    }
  }

  private def reportErrorsFlow[T]: Flow[T, T, Any] =
    Flow[T]
      .watchTermination()((_, f) =>
        f.onComplete {
          case Failure(cause) => log.error(s"WS stream failed with $cause")
          case _              => //ignore
        }(system.dispatcher)
      )

}

object IngestionWebSocketEndpoint extends ConfigSupport {

  import concurrent.duration._

  private[http] val maxNumberOfWSFrames: Int =
    rootConfig.getInt("hydra.ingest.websocket.max-frames")

  private[http] val streamedWSMessageTimeout: FiniteDuration =
    rootConfig
      .getDuration("hydra.ingest.websocket.stream-timeout")
      .toMillis
      .millis

}
