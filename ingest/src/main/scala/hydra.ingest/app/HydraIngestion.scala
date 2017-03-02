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

package hydra.ingest.app

import java.io.File

import akka.actor.Props
import com.github.vonnagy.service.container.ContainerBuilder
import com.pluralsight.hydra.network.util.NetworkUtils
import com.typesafe.config.ConfigFactory
import hydra.common.akka.ActorUtils
import hydra.core.app.HydraEntryPoint
import hydra.ingest.services._
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.health.KafkaHealthCheckActor
import hydra.kafka.services.KafkaProducerSupervisor

/**
  * Created by alexsilva on 2/18/16.
  */
object HydraIngestion extends HydraEntryPoint with KafkaConfigSupport {

  val moduleName = "ingest"

  override val config = rootConfig.withFallback(ConfigFactory.parseFile(new File("/etc/hydra/hydra-ingest.conf")))

  override def beforeStart(builder: ContainerBuilder): ContainerBuilder = {
    builder.validateConfig("hydra.schema.registry.url")
    val lanAddress = NetworkUtils.getLocalAddress.getHostAddress
    log.debug(s"Setting akka.remote.artery.canonical.hostname to $lanAddress")
    val cfg = ConfigFactory.parseString(s"""akka.remote.artery.canonical.hostname="$lanAddress"""")
      .withFallback(builder.config)
    builder.withConfig(cfg)
  }


  override val services = Seq(
    Tuple2(ActorUtils.actorName[KafkaProducerSupervisor], KafkaProducerSupervisor.props(kafkaProducerFormats)),
    Tuple2(ActorUtils.actorName[KafkaHealthCheckActor], Props[KafkaHealthCheckActor]),
    Tuple2(ActorUtils.actorName[IngestorRegistry], Props[IngestorRegistry]),
    Tuple2(ActorUtils.actorName[IngestorRegistrar], Props[IngestorRegistrar]),
    Tuple2(ActorUtils.actorName[IngestionErrorHandler], Props[IngestionErrorHandler]),
    Tuple2(ActorUtils.actorName[IngestionActor], Props(classOf[IngestionActor], "/user/service/ingestor_registry")))

  //++: slackActor.toOption


  doStart()

}
