package hydra.core.akka

import akka.actor.{Actor, Props}
import akka.cluster.ClusterEvent.{ClusterDomainEvent, CurrentClusterState, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, MemberStatus}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class ClusterTestSpec extends MultiNodeSpec(ClusterTestConfig) with STMultiNodeSpec
  with ImplicitSender {

  import ClusterTestConfig._

  def initialParticipants = 2

  "A Cluster" must {
    val node1Addr = node(first).address
    val node2Addr = node(second).address
    val node3Addr = node(third).address

    "let a node join and leave" in {
      println(s"My name is ${myself.name} and my port is ${node(myself).address.port.get}")
      ignoreMsg { case m: CurrentClusterState => println(s"${myself.name} - Current state: ${m}"); true }
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      Cluster(system).subscribe(testActor, classOf[MemberRemoved])
      Cluster(system).subscribe(system.actorOf(Props(classOf[ClusterEventLogger], myself)), classOf[ClusterDomainEvent])

      runOn(first, second) {
        Cluster(system).join(node1Addr)
        receiveN(2).collect { case MemberUp(m) ⇒ m.address }.toSet should be(
          Set(node1Addr, node2Addr))

        runOn(second) {
          Cluster(system).leave(node2Addr)
          expectMsgType[MemberRemoved](10.seconds).member.address should be(node1Addr)
        }

        expectMsgType[MemberRemoved](10.seconds).member.address should be(node2Addr)
      }

      enterBarrier("end")
    }

    "let another node start and then detect its failure" in {
      runOn(third) {
        Cluster(system).join(node1Addr)
        receiveN(2).collect { case MemberUp(m) ⇒ m.address }.toSet should be(
          Set(node1Addr, node3Addr))
      }
      runOn(first) { expectMsgType[MemberUp].member.address should be(node3Addr) }
      enterBarrier("thirdJoined")

      runOn(first) {
        println("disconn")
        testConductor.shutdown(third).await
        val removedMsg = expectMsgType[MemberRemoved](30.seconds)
        removedMsg.member.address should be(node3Addr)
        removedMsg.previousStatus should be(MemberStatus.Down)
      }

      expectNoMessage(3.seconds)
    }
  }
}

class ClusterTestSpecMultiJvmFirst extends ClusterTestSpec
class ClusterTestSpecMultiJvmSecond extends ClusterTestSpec
class ClusterTestSpecMultiJvmThird extends ClusterTestSpec


trait STMultiNodeSpec extends MultiNodeSpecCallbacks
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}

object ClusterTestConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  testTransport(true)

  nodeConfig(first)(ConfigFactory.parseString("akka.remote.netty.tcp.port=2551"))
  nodeConfig(second)(ConfigFactory.parseString("akka.remote.netty.tcp.port=2552"))
  nodeConfig(third)(ConfigFactory.parseString("akka.remote.netty.tcp.port=2553"))

  commonConfig(ConfigFactory.parseString(
    """
	akka {
	  loglevel = INFO
	  stdout-loglevel = INFO
	  loggers = ["akka.testkit.TestEventListener"]
	  log-dead-letters = 0
	  log-dead-letters-during-shutdown = off

	  actor {
	    provider = "akka.cluster.ClusterActorRefProvider"
	  }
	  remote {
	    log-remote-lifecycle-events = off
	    enabled-transports = ["akka.remote.netty.tcp"]
	    netty.tcp {
	      hostname = "127.0.0.1"
	      port = 0
	    }
	  }

      testconductor.barrier-timeout = 60s

	  cluster {
	     # don't use sigar for tests, native lib not in path
	     metrics.collector-class = akka.cluster.JmxMetricsCollector
	     auto-down-unreachable-after = 5s
	  }
	}
"""))
}

class ClusterEventLogger(me: RoleName) extends Actor {
  def receive = {
    case m: ClusterDomainEvent => println(s"${me.name} received ${m}")
  }
}