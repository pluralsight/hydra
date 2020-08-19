package hydra.kafka.algebras

import java.time.Instant

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{ConsumerGroupDetails, TopicConsumerGroups}
import hydra.kafka.algebras.KafkaClientAlgebra.TopicName
import io.chrisdavenport.log4cats.Logger

trait ConsumerGroupsAlgebra[F[_]] {

  def getConsumerGroups(topicName: TopicName): F[TopicConsumerGroups]

  def getAllConsumerGroups: F[List[TopicConsumerGroups]]

}

object ConsumerGroupsAlgebra {
  object TopicConsumerGroups {
    def empty(topicName: TopicName): TopicConsumerGroups = TopicConsumerGroups(topicName, Nil)
  }
  final case class TopicConsumerGroups(topic: TopicName, consumerGroups: List[ConsumerGroupDetails])
  final case class ConsumerGroupDetails(name: String, lastCommit: Instant)

  def make[F[_]: Sync: Concurrent: Logger](): F[ConsumerGroupsAlgebra[F]] = {
    val b = for {
      ref <- Ref[F].of(ConsumerGroupsStorageFacade.empty)
    } yield ref
  }
}

private case class ConsumerGroupsStorageFacade(consumerMap: Map[TopicName, List[ConsumerGroupDetails]]) {
  def getTopicConsumerGroups(topicName: TopicName): TopicConsumerGroups =
    consumerMap.get(topicName).map(TopicConsumerGroups(topicName, _)).getOrElse(TopicConsumerGroups.empty(topicName))
  def getAllTopicConsumerGroups: List[TopicConsumerGroups] = consumerMap.map(c => TopicConsumerGroups(c._1, c._2)).toList
  def addConsumerGroup(topicName: TopicName, consumerGroupDetails: ConsumerGroupDetails): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap + (topicName -> (this.consumerMap.getOrElse(topicName, Nil) :+ consumerGroupDetails)))
  def removeConsumerGroup(topicName: TopicName, consumerGroupName: String): ConsumerGroupsStorageFacade =
    this.copy(this.consumerMap + (topicName -> this.consumerMap.getOrElse(topicName, Nil).filterNot(_.name == consumerGroupName)))
}

private object ConsumerGroupsStorageFacade {
  def empty: ConsumerGroupsStorageFacade = ConsumerGroupsStorageFacade(Map.empty)
}

