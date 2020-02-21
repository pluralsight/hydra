package hydra.kafka.consumer

import akka.actor.Actor
import akka.pattern.pipe
import hydra.kafka.consumer.KafkaConsumerProxy._
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.concurrent.Future

class KafkaConsumerProxy extends Actor {

  private var _defaultConsumer: Consumer[String, String] = _

  private implicit val ec = context.dispatcher

  override def preStart(): Unit = {
    _defaultConsumer = KafkaUtils.stringConsumerSettings.createKafkaConsumer()
  }

  override def receive: Receive = {
    case GetLatestOffsets(topic) =>
      val requestor = sender
      pipe(latestOffsets(topic).map(LatestOffsetsResponse(topic, _))) to requestor

    case GetPartitionInfo(topic) =>
      val requestor = sender
      pipe(partitionInfo(topic).map(PartitionInfoResponse(topic, _))) to requestor

    case ListTopics =>
      val requestor = sender
      pipe(listTopics().map(ListTopicsResponse(_))) to requestor
  }

  override def postStop(): Unit = {
    _defaultConsumer.close()
  }

  private def latestOffsets(
      topic: String
  ): Future[Map[TopicPartition, Long]] = {
    Future {
      val ts = _defaultConsumer
        .partitionsFor(topic)
        .asScala
        .map(pi => new TopicPartition(topic, pi.partition()))
      _defaultConsumer
        .endOffsets(ts.asJava)
        .asScala
        .map(tp => tp._1 -> tp._2.toLong)
        .toMap
    }
  }

  private def partitionInfo(topic: String): Future[Seq[PartitionInfo]] =
    Future(_defaultConsumer.partitionsFor(topic).asScala)

  private def listTopics(): Future[Map[String, Seq[PartitionInfo]]] = {
    Future(_defaultConsumer.listTopics().asScala.toMap)
      .map(res => res.mapValues(_.asScala.toSeq))
  }

}

object KafkaConsumerProxy {

  case class GetLatestOffsets(topic: String)

  case class LatestOffsetsResponse(
      topic: String,
      offsets: Map[TopicPartition, Long]
  )

  case class GetPartitionInfo(topic: String)

  case class PartitionInfoResponse(
      topic: String,
      partitionInfo: Seq[PartitionInfo]
  )

  case object ListTopics

  case class ListTopicsResponse(topics: Map[String, Seq[PartitionInfo]])

}
