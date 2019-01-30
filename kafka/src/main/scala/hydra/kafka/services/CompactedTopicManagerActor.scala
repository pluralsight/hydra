package hydra.kafka.services

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.kafka.model.TopicMetadata
import hydra.kafka.services.CompactedTopicManagerActor._
import hydra.kafka.util.KafkaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class CompactedTopicManagerActor(metadataConsumerActor: ActorRef,
                                 schemaRegistryClient: SchemaRegistryClient,
                                  kafkaConfig: Config,
                                  bootstrapServers: String,
                                  kafkaUtils: KafkaUtils) extends Actor
  with ConfigSupport
  with ActorLogging {

  private final val COMPACTED_PREFIX = "_compacted."
  private implicit val ec = context.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()
  implicit val timeout = Timeout(10.seconds)


  private val topicDetailsConfig: util.Map[String, String] = Map[String, String]("cleanup.policy" -> "compact").asJava
  private final val topicDetails = new TopicDetails(
    kafkaConfig.getInt("partitions"),
    kafkaConfig.getInt("replication-factor").toShort,
    topicDetailsConfig)


  override def receive: Receive = {

    case CreateCompactedTopic(topicName) => {
      createCompactedTopic(topicName + this.COMPACTED_PREFIX).map { _ =>
        self ! CreateCompactedStream(topicName)
      }.recover {
        case e: Exception => throw e
      }
    }

    case CreateCompactedStream(topicName) => {
      pipe(createCompactedStream(topicName)) to sender
    }

    case MetadataTopicCreated(topicMetadata) =>

  }

  private[kafka] def shouldCreateCompacted(topicMetadata: TopicMetadata): Boolean  = {
    val schema: Schema = schemaRegistryClient.getById(topicMetadata.schemaId)
    if schema.fields
  }

  private[kafka] def tryCreateCompactedTopic(topicMetadataRequest: TopicMetadataRequest): Future[Unit] = {
    if (topicMetadataRequest.schema.fields.contains("hydra.key") && topicMetadataRequest.streamType == History) {
      log.debug("Historical Stream with hydra.key found, creating topic...")
      compactedTopicManagerActor ! CreateCompactedTopic(topicMetadataRequest.subject)
    }
    Future.successful()
  }

  private[kafka] def createCompactedTopic(compactedTopic: String): Future[Unit] = {

    val timeout = 2000

    val topicExists = kafkaUtils.topicExists(compactedTopic) match {
      case Success(value) => value
      case Failure(exception) =>
        log.error(s"Unable to determine if topic exists: ${exception.getMessage}")
        return Future.failed(exception)
    }

    // Don't fail when topic already exists
    if (topicExists) {
      log.info(s"Compacted Topic $compactedTopic already exists, proceeding anyway...")
      Future.successful(())
    }

    else {

      val topicFut = kafkaUtils.createTopic(compactedTopic, topicDetails, timeout)
        .map { r =>
          r.all.get(timeout, TimeUnit.MILLISECONDS)
        }
        .map { _ =>
          ()
        }
        .recover {
          case e: Exception => throw e
        }
      topicFut
    }
  }

  private[kafka] def createCompactedStream(topicName: String): Future[Unit] = {
    //do we want to return a future unit? how do we signal to the client that compacted was successful?
    Future {
      log.info(s"Attempting to create compacted stream from $topicName to ${topicName + this.COMPACTED_PREFIX}")
      context.actorOf(CompactedTopicStreamActor.props(topicName, this.COMPACTED_PREFIX + topicName, KafkaUtils.BootstrapServers, kafkaConfig))
    }
  }

}

object CompactedTopicManagerActor {

  case class CreateCompactedStream(topicName: String)
  case class CreateCompactedTopic(topicName: String)

  sealed trait CompactedTopicManagerResult

  def props(metadataConsumerActor: ActorRef,
             kafkaConfig: Config,
            bootstrapServers: String,
            kafkaUtils: KafkaUtils) = {
    Props(classOf[CompactedTopicManagerActor], metadataConsumerActor, kafkaConfig, bootstrapServers, kafkaUtils)
  }

}


