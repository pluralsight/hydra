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

package hydra.ingest.endpoints

import akka.actor._
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.{ExceptionHandler, RequestContext, Route}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.ingest.{HydraRequest, HydraRequestMedatata}
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}
import hydra.ingest.HydraIngestorRegistry

/**
  * Created by alexsilva on 12/22/15.
  */
class IngestionWebSocket(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraJsonSupport with HydraDirectives with HydraIngestorRegistry {

  implicit val materializer = ActorMaterializer()

  val greeterWebSocketService =
    Flow[Message]
      .mapConcat {
        // we match but don't actually consume the text message here,
        // rather we simply stream it back as the tail of the response
        // this means we might start sending the response even before the
        // end of the incoming message has been received
        case tm: TextMessage => TextMessage(Source.single("Hello ") ++ tm.textStream) :: Nil
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }

  override val route: Route =
    path("ingest-socket") {
      handleExceptions(excptHandler) {
        println("RAR")
        handleWebSocketMessages(greeterWebSocketService)
      }
    }


  //todo: add retry strategy, etc. stuff
  private def buildRequest(destination: String, payload: String, ctx: RequestContext) = {
    val metadata: List[HydraRequestMedatata] = List(ctx.request.headers.map(header =>
      HydraRequestMedatata(header.name.toLowerCase, header.value)): _*)
    HydraRequest(destination, payload, metadata)
  }

  val excptHandler = ExceptionHandler {
    case e: IllegalArgumentException => complete(GenericServiceResponse(400, e.getMessage))
    case e: Exception => complete(GenericServiceResponse(ServiceUnavailable.intValue, e.getMessage))
  }
}
