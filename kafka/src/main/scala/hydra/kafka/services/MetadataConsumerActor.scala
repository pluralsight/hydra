package hydra.kafka.services

import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.pipe
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.core.marshallers.{History, HydraJsonSupport}
import hydra.kafka.model.TopicMetadata
import hydra.kafka.services.CompactedTopicManagerActor.CreateCompactedTopic
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.format.ISODateTimeFormat
import spray.json._

import scala.concurrent.ExecutionContext

class MetadataConsumerActor(compactedTopicManager: ActorRef,
                             consumerConfig: Config,
                            bootstrapServers: String,
                            schemaRegistryClient: SchemaRegistryClient,
                            metadataTopicName: String) extends Actor
  with ConfigSupport
  with HydraJsonSupport
  with ActorLogging {

  import MetadataConsumerActor._
  private implicit val ec = context.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()

  private val metadataMap = new collection.mutable.HashMap[String, TopicMetadata]()
  private val metadataStream = MetadataConsumerActor.createMetadataStream(consumerConfig, bootstrapServers,
    schemaRegistryClient, metadataTopicName, self)

  private val COMPACTED_PREFIX = "_compacted."

  override def receive: Receive = Actor.emptyBehavior

  override def preStart(): Unit = {
    context.become(streaming(metadataStream.run()))
  }


  def streaming(stream: (Control, NotUsed)): Receive = {
    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap.toMap)

    case t: TopicMetadata =>
      metadataMap.put(t.id.toString, t)
      maybeCreateCompactedStream(t)

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender
  }


  private[kafka] def maybeCreateCompactedStream(metadata: TopicMetadata): Unit = {
    if(StreamTypeFormat.read(metadata.streamType.toJson) == History) {
      val schema = schemaRegistryClient.getById(metadata.schemaId)
      if (schema.getFields.contains("hydra.key")) {
        log.info(s"Attempting to create compacted stream for $metadata")
        context.actorOf(CompactedTopicStreamActor.props(metadata.subject, COMPACTED_PREFIX+metadata.subject, KafkaUtils.BootstrapServers, consumerConfig))
      }
    }
  }

}

object MetadataConsumerActor {

  private type Stream = RunnableGraph[(Control, NotUsed)]

  case object GetMetadata

  case class GetMetadataResponse(metadata: Map[String, TopicMetadata])

  case object StopStream

  case object StreamStopped

  private[services] def createMetadataStream[K, V](config: Config,
                                           bootstrapSevers: String,
                                           schemaRegistryClient: SchemaRegistryClient,
                                           metadataTopicName: String,
                                           destination: ActorRef)
                                          (implicit ec: ExecutionContext, mat: Materializer): Stream = {

    val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

    val settings = ConsumerSettings(config, new StringDeserializer,
      new KafkaAvroDeserializer(schemaRegistryClient))
      .withBootstrapServers(bootstrapSevers)
      .withGroupId("metadata-consumer-actor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


    Consumer.plainSource(settings, Subscriptions.topics(metadataTopicName))
      .map { msg =>
        val record = msg.value.asInstanceOf[GenericRecord]
        TopicMetadata(
          record.get("subject").toString,
          record.get("schemaId").toString.toInt,
          record.get("streamType").toString,
          record.get("derived").toString.toBoolean,
          record.get("dataClassification").toString,
          record.get("contact").toString,
          Option(record.get("additionalDocumentation")).map(_.toString),
          Option(record.get("notes")).map(_.toString),
          UUID.fromString(record.get("id").toString),
          formatter.parseDateTime(record.get("createdDate").toString)
        )
      }.toMat(Sink.actorRef(destination, StreamStopped))(Keep.both)
  }


  def props(compactedTopicManagerActor: ActorRef,
            consumerConfig: Config,
            bootstrapServers: String,
            schemaRegistryClient: SchemaRegistryClient,
            metadataTopicName: String) = {
    Props(classOf[MetadataConsumerActor], compactedTopicManagerActor, consumerConfig, bootstrapServers, schemaRegistryClient, metadataTopicName)
  }
}


