package hydra.kafka.services

import java.net.InetAddress
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
  private implicit val materializer: Materializer = ActorMaterializer()

  private val metadataTopicName = bootstrapKafkaConfig.get[String]("metadata-topic-name").valueOrElse("_hydra.metadata.topic")
  private val compactedPrefix = bootstrapKafkaConfig.get[String]("compacted-topic-prefix").valueOrElse("_compacted.")
  private val enableCompactedDerivedStream = bootstrapKafkaConfig.get[Boolean]("compacted_topic.enabled").valueOrElse(false)

  private[kafka] val metadataMap = Map[String, TopicMetadata]()
  private val metadataStream = StreamsManagerActor.createMetadataStream(bootstrapKafkaConfig, bootstrapServers,
    schemaRegistryClient, metadataTopicName, self)

  private val kafkaUtils = KafkaUtils()

  override def receive: Receive = Actor.emptyBehavior

  override def preStart(): Unit = {
    context.become(streaming(metadataStream.run(), metadataMap))
  }


  def streaming(stream: (Control, NotUsed), metadataMap: Map[String, TopicMetadata]): Receive = {
    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap)

    case t: TopicMetadata =>
      buildCompactedProps(t).foreach { compactedProps =>
        val childName = compactedPrefix + t.subject
        if (context.child(childName).isEmpty) {
          context.actorOf(compactedProps, name = childName)
        }
      }
      context.become(streaming(stream, metadataMap + (t.subject -> t)))

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender

    case GetStreamActor(actorName: String) =>
      pipe(Future {
        GetStreamActorResponse(context.child(actorName))
      }) to sender
  }

  private[kafka] def buildCompactedProps(metadata: TopicMetadata): Option[Props] = {
    booleanToOption[Props](enableCompactedDerivedStream) { () =>
      kafkaUtils.topicExists(metadata.subject) match {
        case Success(e) if e =>
          booleanToOption[Props](StreamTypeFormat.read(metadata.streamType.toJson) == History) { () =>
            val schema = schemaRegistryClient.getById(metadata.schemaId).toString()
            booleanToOption[Props](schema.contains("hydra.key")) { () =>
              log.info(s"Attempting to create compacted stream for $metadata")
              Some(CompactedTopicStreamActor.props(metadata.subject, compactedPrefix + metadata.subject, bootstrapServers, bootstrapKafkaConfig))
            }
          }
        case Success(e) if !e =>
          log.error(s"Topic ${metadata.subject} does not exist; won't create compacted topic.")
          None
        case Failure(ex) =>
          log.error(ex, s"Unable to create compacted topic for ${metadata.subject}")
          None
      }
    }
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
      }.toMat(Sink.actorRef(destination, StreamStopped))(Keep.both)
  }


  def props(bootstrapKafkaConfig: Config,
            bootstrapServers: String,
            schemaRegistryClient: SchemaRegistryClient) = {
    Props(classOf[StreamsManagerActor], bootstrapKafkaConfig, bootstrapServers, schemaRegistryClient)
  }
}


