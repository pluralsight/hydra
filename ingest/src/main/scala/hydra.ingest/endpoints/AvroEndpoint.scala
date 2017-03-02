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

import java.io.IOException

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.core.avro.JsonToAvroSchemaConverter
import hydra.core.marshallers.{GenericServiceResponse, HydraJsonSupport}

/**
  * Created by alexsilva on 2/13/16.
  */
class AvroEndpoint(implicit system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with HydraJsonSupport {

  override def route: Route = path("avro") {
    post {
      handleExceptions(excptHandler) {
        entity(as[String]) { json =>
          parameters("namespace", "name") { (namespace, name) =>
            val converter = new JsonToAvroSchemaConverter(new ObjectMapper)
            val schema = converter.convert(json, namespace, name)
            complete(OK, schema)
          }
        }
      }
    }
  }

  val excptHandler = ExceptionHandler {
    case e: IllegalArgumentException => complete(BadRequest, GenericServiceResponse(400, e.getMessage))
    case e: IOException =>
      val msg = s"Unable to parse schema : ${e.getMessage}"
      complete(BadRequest, GenericServiceResponse(ServiceUnavailable.intValue, msg))
  }

}