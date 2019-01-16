package hydra.kafka.services

import akka.NotUsed
import akka.actor.{Actor, Props}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, RunnableGraph}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.ExecutionContext

class CompactedTopicStreamActor(fromTopic: String, toTopic: String, bootstrapServers: String, config: Config) extends Actor
  with ConfigSupport {


  private implicit val ec = context.dispatcher

  private implicit val materializer: Materializer = ActorMaterializer()

  private val stream = CompactedTopicStreamActor.createStream(config, bootstrapServers, fromTopic, toTopic)

  override def receive: Receive = {
    Actor.emptyBehavior
  }

  override def preStart(): Unit = {
    context.become(streaming(stream.run()))
  }

  def streaming(stream: (Control, NotUsed)): Receive = {
    Actor.emptyBehavior
  }

}

object CompactedTopicStreamActor {

  private type Stream = RunnableGraph[(Control, NotUsed)]

  case class CreateCompactedStream(topicName: String)

  def props(fromTopic: String, toTopic: String, bootstrapServers: String, config: Config) = {
    Props(classOf[CompactedTopicStreamActor], fromTopic, toTopic, bootstrapServers, config)
  }


  private[services] def createStream[K, V](config: Config,
                                           bootstrapSevers: String,
                                           fromTopic: String,
                                           toTopic: String)
                                          (implicit ec: ExecutionContext, mat: Materializer): Stream = {

    val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

    val consumerSettings = ConsumerSettings(config, None, None)
      .withBootstrapServers(bootstrapSevers)
      .withGroupId(toTopic)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val producerSettings = ProducerSettings(config, None, None)


    Consumer.plainSource(consumerSettings, Subscriptions.topics(fromTopic))
      .map { msg =>
         ProducerMessage.single(new ProducerRecord(toTopic, msg.key(), msg.value())
         )
      }.toMat(Producer.commitableSink(producerSettings))(Keep.both)
  }


}


