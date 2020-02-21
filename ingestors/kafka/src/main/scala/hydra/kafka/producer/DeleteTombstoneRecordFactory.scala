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

package hydra.kafka.producer

import hydra.core.ingest.HydraRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 9/27/16.
  */

object DeleteTombstoneRecordFactory extends KafkaRecordFactory[String, Any] {

  override def build(
      request: HydraRequest
  )(implicit ex: ExecutionContext): Future[DeleteTombstoneRecord] = {
    val theKey = getKey(request)
      .fold[Try[String]](
        Failure(new IllegalArgumentException("A key is required for deletes."))
      )(Success(_))
    for {
      key <- Future.fromTry(theKey)
      topic <- Future.fromTry(getTopic(request))
    } yield DeleteTombstoneRecord(topic, key, request.ackStrategy)
  }
}
