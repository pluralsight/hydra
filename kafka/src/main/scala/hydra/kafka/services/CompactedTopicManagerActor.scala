package hydra.kafka.services

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.kafka.services.CompactedTopicManagerActor._
import hydra.kafka.services.MetadataConsumerActor.{GetMetadata, GetMetadataResponse}
import hydra.kafka.services.TopicBootstrapActor.GetStreams
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails

import scala.concurrent.Future
import scala.util.{Failure, Success}
import akka.pattern.ask

class CompactedTopicManagerActor(metadataConsumerActor: ActorRef,
                                  kafkaConfig: Config,
                                  bootstrapServers: String,
                                  kafkaUtils: KafkaUtils) extends Actor
  with ConfigSupport
  with ActorLogging {

  private final val COMPACTED_PREFIX = "_compacted."
  private implicit val ec = context.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()


  //start all streams from existing metadata
  override def preStart(): Unit = {
    (metadataConsumerActor ? GetMetadata).mapTo[GetMetadataResponse].map {
      gmr =>
        gmr.metadata.map {
          case (key, topicMetadata) => createCompactedStream(this.COMPACTED_PREFIX + topicMetadata.subject)
        }
    }
    super.preStart()
  }

  override def receive: Receive = {

    case CreateCompactedTopic(topicName, topicDetails) => {
      createCompactedTopic(this.COMPACTED_PREFIX + topicName, topicDetails).map { _ =>
        self ! CreateCompactedStream(topicName)
      }.recover {
        case e: Exception => throw e
      }
    }

    case CreateCompactedStream(topicName) => {
      pipe(createCompactedStream(topicName)) to sender
    }


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
      log.info(s"Compacted Topic $topicName already exists, proceeding anyway...")
      Future.successful(())
    }

    else {

      import scala.collection.JavaConverters._
      val topicDetailsConfig: util.Map[String, String] = Map[String, String]("cleanup.policy" -> "compact").asJava
      val compactedDetails = new TopicDetails(topicDetails.numPartitions, topicDetails.replicationFactor, topicDetailsConfig)

      val topicFut = kafkaUtils.createTopic(topicName, compactedDetails, timeout)
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
      log.info(s"Attempting to create compacted stream from $topicName to ${topicName+this.COMPACTED_PREFIX}")
      context.actorOf(CompactedTopicStreamActor.props(topicName, this.COMPACTED_PREFIX + topicName, KafkaUtils.BootstrapServers, kafkaConfig))
    }
  }

}

object CompactedTopicManagerActor {

  case class CreateCompactedStream(topicName: String)
  case class CreateCompactedTopic(topicName: String, topicDetails: TopicDetails)

  sealed trait CompactedTopicManagerResult

  def props(metadataConsumerActor: ActorRef,
             kafkaConfig: Config,
            bootstrapServers: String,
            kafkaUtils: KafkaUtils) = {
    Props(classOf[CompactedTopicManagerActor], metadataConsumerActor, kafkaConfig, bootstrapServers, kafkaUtils)
  }

}


