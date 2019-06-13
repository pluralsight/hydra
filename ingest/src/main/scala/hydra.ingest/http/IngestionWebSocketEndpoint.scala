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
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Flow
import akka.stream.{Materializer, StreamLimitReachedException}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.GenericServiceResponse
import hydra.core.protocol.HydraApplicationError
import hydra.ingest.services.{IngestSocketFactory, IngestionOutgoingMessage, SimpleOutgoingMessage}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.NonFatal

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionWebSocketEndpoint(implicit system: ActorSystem, implicit val e: ExecutionContext, materizlizer: Materializer)
  extends RoutedEndpoints with LoggingAdapter with HydraIngestJsonSupport with HydraDirectives {

  //visible for testing
  private[http] val enabled = applicationConfig.get[Boolean]("ingest.websocket.enabled")
    .valueOrElse(false)

  private val socketFactory = IngestSocketFactory.createSocket(system)
  implicit val simpleOutgoingMessageFormat: JsonFormat[SimpleOutgoingMessage] = jsonFormat2(SimpleOutgoingMessage)

  override val route: Route =
    path("ws-ingest") {
      if (enabled) {
        handleWebSocketMessages(ingestSocketFlow())
      }
      else {
        complete(StatusCodes.Conflict, GenericServiceResponse(409, "Web Socket not available."))
      }
    }


  private[http] def ingestSocketFlow(): Flow[Message, Message, Any] = {
    Flow[Message].collect {
      case TextMessage.Strict(txt) => Future successful txt
      case TextMessage.Streamed(src) => src
        .limit(IngestionWebSocketEndpoint.maxNumberOfWSFrames)
        .recover {
          case s: StreamLimitReachedException =>
            log.error("Stream frame limit reached after frame number {}", s.n)
          case NonFatal(t) =>
            log.error("Exception in combining websocket frames, {}", t.getMessage)
        }
        .completionTimeout(IngestionWebSocketEndpoint.streamedWSMessageTimeout)
        .runFold("")(_ + _)
    }.mapAsync(1)(identity).via(socketFactory.ingestFlow()).map {
      case m: SimpleOutgoingMessage => TextMessage(m.toJson.compactPrint)
      case r: IngestionOutgoingMessage => TextMessage(r.report.toJson.compactPrint)
    }.via(reportErrorsFlow)
  }

  private def reportErrorsFlow[T]: Flow[T, T, Any] =
    Flow[T]
      .watchTermination()((_, f) => f.onComplete {
        case Failure(cause) => log.error(s"WS stream failed with $cause")
        case _ => //ignore
      }(e))

}

object IngestionWebSocketEndpoint extends ConfigSupport {

  import concurrent.duration._

  private[http] val maxNumberOfWSFrames: Int =
    rootConfig.getInt("hydra.ingest.websocket.max-frames")

  private[http] val streamedWSMessageTimeout: FiniteDuration =
    rootConfig.getDuration("hydra.ingest.websocket.stream-timeout").toMillis.millis

}
