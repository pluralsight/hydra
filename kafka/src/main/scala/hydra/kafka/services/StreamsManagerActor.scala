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
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.core.marshallers.{History, HydraJsonSupport}
import hydra.kafka.model.TopicMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.format.ISODateTimeFormat
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class StreamsManagerActor(bootstrapKafkaConfig: Config,
                          bootstrapServers: String,
                          schemaRegistryClient: SchemaRegistryClient,
                          ) extends Actor
  with ConfigSupport
  with HydraJsonSupport
  with ActorLogging {

  import StreamsManagerActor._
  private implicit val ec = context.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()

  private val metadataTopicName = bootstrapKafkaConfig.get[String]("metadata-topic-name").valueOrElse("_hydra.metadata.topic")
  private val compactedPrefix = bootstrapKafkaConfig.get[String]("compacted-topic-prefix").valueOrElse("_compacted.")

  private val metadataMap = new collection.mutable.HashMap[String, TopicMetadata]()
  private val metadataStream = StreamsManagerActor.createMetadataStream(bootstrapKafkaConfig, bootstrapServers,
    schemaRegistryClient, metadataTopicName, self)


  override def receive: Receive = Actor.emptyBehavior

  override def preStart(): Unit = {
    context.become(streaming(metadataStream.run()))
  }


  def streaming(stream: (Control, NotUsed)): Receive = {
    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap.toMap)

    case t: TopicMetadata =>
      metadataMap.put(t.id.toString, t)
       buildCompactedProps(t).foreach { compactedProps =>
         Try(
           context.actorOf(compactedProps, name = compactedPrefix + t.subject)
         ).recover {
           case e: Throwable => log.info(s"attemped to create existing compacted topic stream actor $e")
         }

       }

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender

    case GetStreamActor(actorName: String) =>
      pipe(Future{GetStreamActorResponse(context.child(actorName))}) to sender
  }


  private[kafka] def buildCompactedProps(metadata: TopicMetadata): Option[Props] = {
    if(StreamTypeFormat.read(metadata.streamType.toJson) == History) {
      val schema = schemaRegistryClient.getById(metadata.schemaId).toString()
      if (schema.contains("hydra.key")) {
        log.info(s"Attempting to create compacted stream for $metadata")
        return Some(CompactedTopicStreamActor.props(metadata.subject, compactedPrefix+metadata.subject, bootstrapServers, bootstrapKafkaConfig))
      }
    }
    None
  }

}

object StreamsManagerActor {

  private type Stream = RunnableGraph[(Control, NotUsed)]

  case object GetMetadata

  case class GetStreamActor(actorName: String)
  case class GetMetadataResponse(metadata: Map[String, TopicMetadata])
  case class GetStreamActorResponse(actor: Option[ActorRef])

  case object StopStream

  case object StreamStopped

  def getMetadataTopicName(c: Config) = c.get[String]("metadata-topic-name")
    .valueOrElse("_hydra.metadata.topic")

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


  def props(bootstrapKafkaConfig: Config,
            bootstrapServers: String,
            schemaRegistryClient: SchemaRegistryClient) = {
    Props(classOf[StreamsManagerActor], bootstrapKafkaConfig, bootstrapServers, schemaRegistryClient)
  }
}


