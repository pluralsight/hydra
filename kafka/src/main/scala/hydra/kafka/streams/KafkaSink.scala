package hydra.kafka.streams

import akka.actor.ActorRef
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.CorrelationIdBuilder

class KafkaSink[K, V](settings: ProducerSettings[K, V])
                     (implicit mat: Materializer) extends LoggingAdapter {

  private val id = settings.properties.get("client.id").getOrElse(CorrelationIdBuilder.generate(10))

  private lazy val streamActor: ActorRef =
    Source.actorRef(Int.MaxValue, OverflowStrategy.fail)
      .via(Producer.flow(settings))
      .log(s"Kafka Sink[$id]")
      .toMat(Sink.ignore)(Keep.left).run()

}