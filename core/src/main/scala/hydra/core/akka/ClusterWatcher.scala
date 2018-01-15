package hydra.core.akka

import akka.actor.Actor
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import hydra.core.akka.ClusterWatcher.{GetNodes, GetNodesByRole}

class ClusterWatcher extends Actor {

  private val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])

  override def postStop(): Unit = cluster unsubscribe self

  private[akka] var nodes = Set.empty[Member]

  //for testing
  def getNodes: Seq[Member] =nodes.toSeq


  override def receive = {
    case state: CurrentClusterState =>
      nodes = state.members.collect { case m if m.status == MemberStatus.Up => m }

    case MemberUp(member) =>
      nodes += member

    case MemberRemoved(member, _) =>
      nodes -= member

    case GetNodes =>
      sender ! nodes.map(_.address).toList

    case GetNodesByRole(role) =>
      sender ! nodes.filter(_.hasRole(role)).map(_.address).toList

    case _: MemberEvent ⇒ // ignore
  }
}

object ClusterWatcher {

  case object GetNodes

  case class GetNodesByRole(role: String)

}