package akka.cluster

import akka.actor.{ActorSystem, Address, PoisonPill, Props}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, MemberUp, MemberWeaklyUp}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.akka.ClusterWatcher
import hydra.core.akka.ClusterWatcher.{GetNodes, GetNodesByRole}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

class ClusterWatchSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load())))
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll
  with BeforeAndAfterEach {


  var longUid = 0L

  def host = {
    longUid += 1
    s"member-$longUid"
  }

  def address = UniqueAddress(Address("akka.tcp", "hydra", host, 1234), longUid)

  override def beforeAll = {
    //need to shut this down so it stops send events that have no members
    system.actorSelection("akka://hydra/system/cluster/core/publisher") ! PoisonPill
  }

  override def afterAll = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "The ClusterWatchSpec" should "add members to list" in {
    val probe = TestProbe()
    val a = address
    val listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])
    listener ! MemberUp(new Member(a, 1, MemberStatus.Up, Set("dc-test", "test")))
    awaitCond(!listener.underlyingActor.getNodes.isEmpty)
    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF(max = 10.seconds) {
      case n :: Nil =>
        n.asInstanceOf[Address] shouldBe a.address
    }
  }

  it should "list members by role" in {
    val listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])
    val probe = TestProbe()
    val address2 = address
    val address1 = address
    listener ! MemberUp(new Member(address1, 2, MemberStatus.Up, Set("dc-test", "role1")))
    listener ! MemberUp(new Member(address2, 3, MemberStatus.Up, Set("dc-test", "role2")))
    awaitCond(listener.underlyingActor.getNodes.length == 2, max = 5.seconds)
    listener.tell(GetNodesByRole("role2"), probe.ref)
    probe.expectMsgPF(max = 5.seconds) {
      case n :: Nil =>
        n.asInstanceOf[Address] shouldBe address2.address
    }
  }

  it should "accept cluster state messages" in {
    val probe = TestProbe()
    val a = address
    val listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])
    val members = SortedSet(new Member(a, 4, MemberStatus.Up, Set("dc-test", "role1")))
    listener ! CurrentClusterState(members)
    awaitCond(listener.underlyingActor.getNodes.length == 1, max = 15.seconds)
    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF() {
      case n1 :: Nil =>
        n1.asInstanceOf[Address] shouldBe a.address
    }
  }

  it should "ignore MemberWeaklyUp events" in {
    val probe = TestProbe()
    val listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])

    listener.tell(MemberWeaklyUp(new Member(address, 5, MemberStatus.WeaklyUp,
      Set("dc-test", "test"))), probe.ref)
    probe.expectNoMessage(1.second)
  }

  it should "remove members from list" in {
    val probe = TestProbe()
    val addr = address
    val listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])
    listener ! MemberUp(new Member(addr, 6, MemberStatus.Up, Set("dc-test", "test")))
    awaitCond(listener.underlyingActor.getNodes.length == 1, max = 5.seconds)

    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF() {
      case n :: Nil =>
        n.asInstanceOf[Address].system shouldBe "hydra"
    }

    listener ! MemberRemoved(new Member(addr, 7, MemberStatus.Removed, Set("dc-test")),
      MemberStatus.Up)
    awaitCond(listener.underlyingActor.getNodes.isEmpty, max = 5.seconds)

    listener.tell(GetNodes, probe.ref)
    probe.expectMsg(Nil)
  }
}
