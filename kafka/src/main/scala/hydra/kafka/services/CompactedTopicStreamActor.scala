package hydra.kafka.services

import java.util
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorLogging, Props}
import akka.event.Logging
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Keep, RunnableGraph}
import akka.stream.{ActorMaterializer, Attributes, Materializer}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.core.HydraException
import hydra.kafka.services.CompactedTopicStreamActor.CompactedTopicCreationException
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class CompactedTopicStreamActor(fromTopic: String, toTopic: String, bootstrapServers: String, kafkaConfig: Config) extends Actor
  with ConfigSupport
  with ActorLogging {


  private implicit val ec = context.dispatcher

  private implicit val materializer: Materializer = ActorMaterializer()

  private val stream = CompactedTopicStreamActor.createStream(kafkaConfig, bootstrapServers, fromTopic, toTopic)
  private val kafkaUtils = KafkaUtils()

  private val compactedDetailsConfig: util.Map[String, String] = Map[String, String]("cleanup.policy" -> "compact").asJava
  private final val compactedDetails = new TopicDetails(
    kafkaConfig.getInt("partitions"),
    kafkaConfig.getInt("replication-factor").toShort,
    compactedDetailsConfig)

  override def receive: Receive = {
    Actor.emptyBehavior
  }

  override def preStart(): Unit = {
    log.debug(s"Starting compacted topic actor for $toTopic")
    kafkaUtils.topicExists(self.path.name).collect {
      case true => {
        context.become(streaming(stream.run()))
      }
      case false => {
        val timeoutMillis = kafkaConfig.getInt("timeout")
        kafkaUtils.createTopic(self.path.name, compactedDetails, timeoutMillis).map {
          result => result.all.get(timeoutMillis, TimeUnit.MILLISECONDS)
        }.map { _ =>
          context.become(streaming(stream.run()))
        }.recover {
          case e => throw CompactedTopicCreationException("Couldn't create compacted topic, but was needed for compacted stream...", e)
        }
      }
    }.recover {
      case e => throw e
    }

  }

  def streaming(stream: Consumer.DrainingControl[Done]): Receive = {
    Actor.emptyBehavior
  }


}

object CompactedTopicStreamActor {

  private type Stream = RunnableGraph[DrainingControl[Done]]

  case class CreateCompactedStream(topicName: String)

  case class CompactedTopicCreationException(message: String, e: Throwable) extends HydraException(message, e)

  def props(fromTopic: String, toTopic: String, bootstrapServers: String, config: Config) = {
    Props(classOf[CompactedTopicStreamActor], fromTopic, toTopic, bootstrapServers, config)
  }


  private[services] def createStream[K, V](config: Config,
                                           bootstrapSevers: String,
                                           fromTopic: String,
                                           toTopic: String)
                                          (implicit ec: ExecutionContext, mat: Materializer): Stream = {

    val consumerSettings = ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapSevers)
      .withGroupId(toTopic)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val producerSettings = ProducerSettings(config, new StringSerializer, new ByteArraySerializer).withBootstrapServers(bootstrapSevers)
    val committerSettings = CommitterSettings(config)


    val stream: RunnableGraph[DrainingControl[Done]] = Consumer.committableSource(consumerSettings, Subscriptions.topics(fromTopic))
      .map({ msg =>
        ProducerMessage.single(new ProducerRecord(toTopic, msg.record.key, msg.record.value),
          passThrough = msg.committableOffset
        )
      })
      .log(s"compacted stream logging: $fromTopic")
        .withAttributes(
          Attributes.logLevels(
            onElement = Logging.InfoLevel,
            onFinish = Logging.InfoLevel,
            onFailure = Logging.DebugLevel
          )
        )
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)

    stream
  }


}


