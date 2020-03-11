package hydra.ingest.services

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe

class IngestionSocketActorSpec
    extends AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem()

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def getIngestActorRef = system.actorOf(Props[IngestionSocketActor])

  it should "ack the init message in waiting state" in {
    val ingestActor = getIngestActorRef
    val probe = TestProbe()
    ingestActor.tell(SocketInit, probe.ref)
    probe.expectMsg(SocketAck)
  }

  it should "ack the init message in initialized state" in {
    val ingestActor = getIngestActorRef
    val probe = TestProbe()
    ingestActor ! SocketStarted(probe.ref)
    ingestActor.tell(SocketInit, probe.ref)
    probe.expectMsg(SocketAck)
  }

  private def testIngestionMessageAck(ingestionMessages: IncomingMessage*) = {
    it should s"ack the incoming messages of form: $ingestionMessages" in {
      val ingestActor = getIngestActorRef
      val probe = TestProbe()
      ingestActor ! SocketStarted(probe.ref)
      ingestActor.tell(SocketInit, probe.ref)
      probe.expectMsg(SocketAck)
      ingestionMessages.foreach { ingestionMessage =>
        ingestActor.tell(ingestionMessage, probe.ref)
        probe.expectMsgClass(classOf[SimpleOutgoingMessage])
        probe.expectMsg(SocketAck)
      }
    }
  }

  testIngestionMessageAck(IncomingMessage("-c HELP"))
  testIngestionMessageAck(IncomingMessage("-c SET hydra-ack = replicated"))
  testIngestionMessageAck(IncomingMessage("-c WHAT"))

}
