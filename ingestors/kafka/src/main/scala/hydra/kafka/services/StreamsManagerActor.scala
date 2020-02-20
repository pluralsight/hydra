package hydra.kafka.services

import java.net.InetAddress
import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.pipe
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.common.util.MonadUtils.booleanToOption
import hydra.core.marshallers.{History, HydraJsonSupport}
import hydra.kafka.model.TopicMetadata
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.format.ISODateTimeFormat
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class StreamsManagerActor(bootstrapKafkaConfig: Config,
                          bootstrapServers: String,
                          schemaRegistryClient: SchemaRegistryClient,
                         ) extends Actor
  with ConfigSupport
  with HydraJsonSupport
  with ActorLogging {

  import StreamsManagerActor._

  private implicit val ec = context.dispatcher

  private implicit val system = context.system

  private val metadataTopicName = bootstrapKafkaConfig.get[String]("metadata-topic-name").valueOrElse("_hydra.metadata.topic")

  private[kafka] val metadataMap = Map[String, TopicMetadata]()
  private val metadataStream = StreamsManagerActor.createMetadataStream(bootstrapKafkaConfig, bootstrapServers,
    schemaRegistryClient, metadataTopicName, self)

  override def receive: Receive = Actor.emptyBehavior

  override def preStart(): Unit = {
    context.become(streaming(metadataStream.run(), metadataMap))
  }


  def streaming(stream: (Control, NotUsed), metadataMap: Map[String, TopicMetadata]): Receive = {
    case InitializedStream =>
      sender ! MetadataProcessed

    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap)

    case t: TopicMetadata =>
      context.become(streaming(stream, metadataMap + (t.subject -> t)))
      sender ! MetadataProcessed

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender

    case GetStreamActor(actorName: String) =>
      pipe(Future {
        GetStreamActorResponse(context.child(actorName))
      }) to sender

    case StreamFailed(ex) => log.error("StreamsManagerActor stream failed with exception", ex)
  }
}

object StreamsManagerActor {

  private type Stream = RunnableGraph[(Control, NotUsed)]

  case object GetMetadata

  case object MetadataProcessed

  case class GetStreamActor(actorName: String)

  case class GetMetadataResponse(metadata: Map[String, TopicMetadata])

  case class GetStreamActorResponse(actor: Option[ActorRef])

  case object StopStream

  case object StreamStopped

  case class StreamFailed(ex: Throwable)

  case object InitializedStream

  def getMetadataTopicName(c: Config) = c.get[String]("metadata-topic-name")
    .valueOrElse("_hydra.metadata.topic")

  private[services] def createMetadataStream[K, V](config: Config,
                                                   bootstrapSevers: String,
                                                   schemaRegistryClient: SchemaRegistryClient,
                                                   metadataTopicName: String,
                                                   destination: ActorRef)
                                                  (implicit ec: ExecutionContext, s: ActorSystem): Stream = {

    val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

    val maybeHost = Try {
      InetAddress.getLocalHost.getHostName.split("\\.")(0)
    }.getOrElse(UUID.randomUUID().toString)

    val settings = ConsumerSettings(config, new StringDeserializer,
      new KafkaAvroDeserializer(schemaRegistryClient))
      .withBootstrapServers(bootstrapSevers)
      .withGroupId(s"metadata-consumer-actor-$maybeHost")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")


    Consumer.plainSource(settings, Subscriptions.topics(metadataTopicName))
      .map { msg =>
        val record = msg.value.asInstanceOf[GenericRecord]
        TopicMetadata(
          record.get("subject").toString,
          record.get("schemaId").toString.toInt,
          record.get("streamType").toString,
          record.get("derived").toString.toBoolean,
          Option(record.get("deprecated")).map(_.toString.toBoolean),
          record.get("dataClassification").toString,
          record.get("contact").toString,
          Option(record.get("additionalDocumentation")).map(_.toString),
          Option(record.get("notes")).map(_.toString),
          UUID.fromString(record.get("id").toString),
          formatter.parseDateTime(record.get("createdDate").toString)
        )
      }.toMat(Sink.actorRefWithBackpressure(
        destination,
        onInitMessage = InitializedStream,
        ackMessage = MetadataProcessed,
        onCompleteMessage = StreamStopped,
        onFailureMessage = StreamFailed.apply))(Keep.both)
  }


  def props(bootstrapKafkaConfig: Config,
            bootstrapServers: String,
            schemaRegistryClient: SchemaRegistryClient) = {
    Props(classOf[StreamsManagerActor], bootstrapKafkaConfig, bootstrapServers, schemaRegistryClient)
  }
}


