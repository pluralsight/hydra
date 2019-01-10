package hydra.kafka.services

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, Props}
import akka.kafka.scaladsl.Consumer.Control
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.services.CompactedTopicManagerActor.{CompactedTopicManagerResult, _}
import hydra.kafka.services.TopicBootstrapActor.{BootstrapFailure, BootstrapSuccess}
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success}

class CompactedTopicManagerActor(consumerConfig: Config,
                            bootstrapServers: String,
                            schemaRegistryClient: SchemaRegistryClient,
                            metadataTopicName: String,
                                 kafkaUtils: KafkaUtils) extends Actor
  with ConfigSupport
  with ActorLogging {

  import MetadataConsumerActor._

  //private val metadataMap = new collection.mutable.HashMap[String, TopicMetadata]()

  private implicit val ec = context.dispatcher

  private implicit val materializer: Materializer = ActorMaterializer()

  private val stream = MetadataConsumerActor.createStream(consumerConfig, bootstrapServers,
    schemaRegistryClient, metadataTopicName, self)

  override def receive: Receive = {
    case CreateCompactedStream(topicName) => //create the compacted stream if it doesn't exist already
    case CreateCompactedTopic(topicName, topicDetails) => //create the compacted topic if it doesn't exist already
  }

  override def preStart(): Unit = {
    context.become(streaming(stream.run()))
  }


  def streaming(stream: (Control, NotUsed)): Receive = {
    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap.toMap)

    case t: TopicMetadata =>
      metadataMap.put(t.id.toString, t)

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender
  }

  private[kafka] def createCompactedTopic(topicName: String, topicDetails: TopicDetails): Future[Unit] = {

    val timeout = 2000
    val topicExists = kafkaUtils.topicExists(topicName) match {
      case Success(value) => value
      case Failure(exception) =>
        log.error(s"Unable to determine if topic exists: ${exception.getMessage}")
        return Future.failed(exception)
    }

    // Don't fail when topic already exists
    if (topicExists) {
      log.info(s"Topic $topicName already exists, proceeding anyway...")
      Future.successful(())
    }

    else {
      kafkaUtils.createTopic(topicName, topicDetails, timeout)
        .map { r =>
          r.all.get(timeout, TimeUnit.MILLISECONDS)
        }
        .map { _ =>
          ()
        }
        .recover {
          case e: Exception => throw e
        }
    }
  }

}

object CompactedTopicManagerActor {

  case class CreateCompactedStream(topicName: String)
  case class CreateCompactedTopic(topicName: String)

  sealed trait CompactedTopicManagerResult

  def props(consumerConfig: Config,
            bootstrapServers: String,
            schemaRegistryClient: SchemaRegistryClient,
            metadataTopicName: String) = {
    Props(classOf[CompactedTopicManagerActor], consumerConfig, bootstrapServers, schemaRegistryClient, metadataTopicName)
  }

  /*
  private type Stream = RunnableGraph[(Control, NotUsed)]

  case object GetMetadata

  case class GetMetadataResponse(metadata: Map[String, TopicMetadata])

  case object StopStream

  case object StreamStopped

  private[services] def createStream[K, V](config: Config,
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
          formatter.parseDateTime(record.get("createdDate").toString),
        )
      }.toMat(Sink.actorRef(destination, StreamStopped))(Keep.both)
  }



  */

}


