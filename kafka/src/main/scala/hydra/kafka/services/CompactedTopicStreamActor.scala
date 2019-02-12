package hydra.kafka.services

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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContext

class CompactedTopicStreamActor(fromTopic: String, toTopic: String, bootstrapServers: String, config: Config) extends Actor
  with ConfigSupport
  with ActorLogging {


  private implicit val ec = context.dispatcher

  private implicit val materializer: Materializer = ActorMaterializer()

  private val stream = CompactedTopicStreamActor.createStream(config, bootstrapServers, fromTopic, toTopic)

  override def receive: Receive = {
    Actor.emptyBehavior
  }

  override def preStart(): Unit = {
    log.debug(s"Starting compacted topic actor for $toTopic")
    println("we're created!!!!")
    context.become(streaming(stream.run()))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    message match {
      case Some(msg) => log.error(s"compacted topic actor $toTopic failed for $msg, attempting to restart...")
      case None => log.error(s"compacted topic actor failed for unknown reason, attempting restart...")
    }
    super.preRestart(reason, message)
  }

  def streaming(stream: Consumer.DrainingControl[Done]): Receive = {
    Actor.emptyBehavior
  }

}

object CompactedTopicStreamActor {

  private type Stream = RunnableGraph[DrainingControl[Done]]

  case class CreateCompactedStream(topicName: String)

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


