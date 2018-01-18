package akka.cluster

import akka.actor.{ActorSystem, Address, Props}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, MemberUp, MemberWeaklyUp}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.core.akka.ClusterWatcher
import hydra.core.akka.ClusterWatcher.{GetNodes, GetNodesByRole}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.util.Random

class ClusterWatchSpec extends TestKit(ActorSystem("hydra",
  config = ConfigFactory.parseString("akka.actor.provider=cluster")
    .withFallback(ConfigFactory.load()))) with Matchers with FlatSpecLike
  with BeforeAndAfterAll with BeforeAndAfterEach {

  def host = Random.alphanumeric.dropWhile(_.isDigit) take 10 mkString

  def address = UniqueAddress(Address("akka", "hydra", host, 1234), Math.abs(Random.nextLong))

  var listener: TestActorRef[ClusterWatcher] = _

  override def afterAll = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  override def beforeEach = {
    listener = TestActorRef[ClusterWatcher](Props[ClusterWatcher])
  }

  override def afterEach = {
    system.stop(listener)
  }

  "The ClusterWatchSpec" should "add members to list" in {
    val probe = TestProbe()
    val a = address
    listener ! MemberUp(new Member(a, 1, MemberStatus.Up, Set("dc-test", "test")))
    awaitCond(!listener.underlyingActor.getNodes.isEmpty)
    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF(max = 10.seconds) {
      case n :: Nil =>
        n.asInstanceOf[Address] shouldBe a.address
    }
  }

  it should "list members by role" in {
    val probe = TestProbe()
    val address2 = address
    listener ! MemberUp(new Member(address, 2, MemberStatus.Up, Set("dc-test", "role1")))
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
    val members = SortedSet(new Member(a, 2, MemberStatus.Up, Set("dc-test", "role1")))
    listener ! CurrentClusterState(members)
    awaitCond(listener.underlyingActor.getNodes.length == 1, max = 5.seconds)
    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF() {
      case n1 :: Nil =>
        n1.asInstanceOf[Address] shouldBe a.address
    }
  }

  it should "ignore MemberWeaklyUp events" in {
    val probe = TestProbe()
    listener.tell(MemberWeaklyUp(new Member(address, 4, MemberStatus.WeaklyUp,
      Set("dc-test", "test"))), probe.ref)
    probe.expectNoMessage(1.second)
  }

  it should "remove members to list" in {
    val probe = TestProbe()
    val addr = address
    listener ! MemberUp(new Member(addr, 4, MemberStatus.Up, Set("dc-test", "test")))
    awaitCond(listener.underlyingActor.getNodes.length == 1, max = 5.seconds)

    listener.tell(GetNodes, probe.ref)
    probe.expectMsgPF() {
      case n :: Nil =>
        n.asInstanceOf[Address].system shouldBe "hydra"
    }

    listener ! MemberRemoved(new Member(addr, 4, MemberStatus.Removed, Set("dc-test")),
      MemberStatus.Up)
    awaitCond(listener.underlyingActor.getNodes.isEmpty, max = 5.seconds)

    listener.tell(GetNodes, probe.ref)
    probe.expectMsg(Nil)

  }
}
