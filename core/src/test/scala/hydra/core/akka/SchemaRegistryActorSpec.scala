package hydra.core.akka

import java.net.ConnectException

import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.protocol.HydraApplicationError
import org.apache.avro.Schema.Parser
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 3/9/17.
  */
class SchemaRegistryActorSpec
  extends TestKit(ActorSystem("SchemaRegistryActorSpec", config = ConfigFactory.parseString("akka.actor.provider=cluster")))
    with Matchers
    with FlatSpecLike
    with ImplicitSender
    with Eventually
    with BeforeAndAfterAll {

  implicit val timeout = Timeout(3.seconds)

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true,
    duration = 10 seconds)

  val client = ConfluentSchemaRegistry.mockRegistry

  val testSchemaString = Source.fromResource("schema.avsc").mkString
  val testSchema = new Parser().parse(testSchemaString)

  override def beforeAll() = {
    client.register("hydra.test.Tester-value", testSchema)
  }

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[HydraApplicationError])

  val settings = new CircuitBreakerSettings(ConfigFactory.empty) {
    override val maxFailures = 2
    override val callTimeout = 1 second
    override val resetTimeout = 3 seconds
  }

  def fixture() = {
    val config = ConfigFactory.parseString(
      """
        |schema.registry.url = "mock"
      """.stripMargin)
    val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    val testProbe = TestProbe()
    (testProbe, schemaRegistryActor)
  }

  "Receiving FetchSchemaRequest" should "open the circuit breaker when the registry is down" in {
    val config = ConfigFactory.parseString(
      """
        |schema.registry.url = "http://localhost:0101"
      """.stripMargin)
    val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    schemaRegistryActor ! FetchSchemaRequest("test")
    schemaRegistryActor ! FetchSchemaRequest("test")
    schemaRegistryActor ! FetchSchemaRequest("test")
    listener.expectMsgType[HydraApplicationError]
    expectMsgPF() {
      case Failure(ex) => ex shouldBe a[ConnectException]
    }
  }

  it should "not open the circuit for looking up schemas that don't exist" in {
    val (probe, schemaRegistryActor) = fixture

    schemaRegistryActor.tell(FetchSchemaRequest("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    schemaRegistryActor.tell(FetchSchemaRequest("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    schemaRegistryActor.tell(FetchSchemaRequest("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    schemaRegistryActor.tell(FetchSchemaRequest("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    listener.expectNoMessage(3.seconds)
  }

  it should "respond with FetchSchemaResponse" in {
    val (probe, schemaRegistryActor) = fixture

    schemaRegistryActor.tell(FetchSchemaRequest("hydra.test.Tester"), probe.ref)
    probe.expectMsgPF() {
      case FetchSchemaResponse(schema) =>
        schema shouldBe SchemaResource(1, 1, testSchema)
    }
    listener.expectNoMessage(3.seconds)
  }

  "Receiving RegisterSchemaRequest" should "respond with RegisterSchemaResponse" in {
    val (probe, schemaRegistryActor) = fixture

    schemaRegistryActor.tell(RegisterSchemaRequest(testSchemaString), probe.ref)
    probe.expectMsgPF() {
      case RegisterSchemaResponse(schemaResource) =>
        schemaResource.schema.getFullName shouldBe "hydra.test.Tester"
        schemaResource.id should be > 0
        schemaResource.version should be > 0
        schemaResource.schema shouldBe testSchema
    }
  }

  it should "send a SchemaRegistered message to the mediator if registered successfully" in {
    val (probe, schemaRegistryActor) = fixture
    val mediatorListener = TestProbe()

    val mediator = DistributedPubSub(system).mediator
    mediator ! Subscribe(SchemaRegistryActor.SchemaRegisteredTopic, mediatorListener.ref)

    schemaRegistryActor.tell(RegisterSchemaRequest(testSchemaString), probe.ref)

    mediatorListener.expectMsgPF() {
      case SchemaRegistered(schemaResource) =>
        schemaResource.schema shouldBe testSchema
      case _ =>
        fail("Expected SchemaRegistered message through mediator")
    }

  }
  it should "NOT send a SchemaRegistered message to the mediator if NOT registered successfully" in {
    val config = ConfigFactory.parseString(
      """
        |schema.registry.url = "http://localhost:0101"
      """.stripMargin)
    val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    val mediatorListener = TestProbe()
    val senderProbe = TestProbe()
    val mediator = DistributedPubSub(system).mediator

    mediator ! Subscribe(SchemaRegistryActor.SchemaRegisteredTopic, mediatorListener.ref)

    val registerSchemaRequest = schemaRegistryActor ? RegisterSchemaRequest(testSchemaString)

    senderProbe.expectNoMessage(3.seconds)
    mediatorListener.expectNoMessage(3.seconds)
  }

  "Receiving RegisterSchemaResponse" should "save the schema metadata to the cache" in {
    val config = ConfigFactory.parseString(
      """
        |schema.registry.url = "http://localhost:0101"
      """.stripMargin)
    val schemaRegistryActor = system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    val senderProbe = TestProbe()

    val expectedResource = SchemaResource(1, 1, testSchema)
    schemaRegistryActor.tell(SchemaRegistered(expectedResource), senderProbe.ref)
    senderProbe.expectMsgPF() {
      case _ => {}
    }
    schemaRegistryActor.tell(FetchSchemaRequest("hydra.test.Tester"), senderProbe.ref)

    senderProbe.expectMsgPF() {
      case FetchSchemaResponse(actualSchema) =>
        actualSchema shouldBe SchemaResource(1, 1, testSchema)
    }
  }

  "The CircuitBreakerSettings" should "load settings from config" in {
    val cfg = ConfigFactory.parseString(
      """
        |schema-fetcher.max-failures = 10
        |schema-fetcher.call-timeout = 10s
        |schema-fetcher.reset-timeout = 20s
      """.stripMargin)

    val breakerSettings = new CircuitBreakerSettings(cfg)
    breakerSettings.resetTimeout shouldBe 20.seconds
    breakerSettings.maxFailures shouldBe 10
    breakerSettings.callTimeout shouldBe 10.seconds
  }

  it should "load have default values in" in {
    val breakerSettings = new CircuitBreakerSettings(ConfigFactory.empty)
    breakerSettings.resetTimeout shouldBe 30.seconds
    breakerSettings.maxFailures shouldBe 5
    breakerSettings.callTimeout shouldBe 5.seconds
  }

  "addSchemaSuffix" should "add schema suffix if it does not exist" in {
    val subjectWithoutSuffix = "test.namespace-value.Test"
    val subjectWithSuffix = "test.namespace-value.Test-value"

    SchemaRegistryActor.addSchemaSuffix(subjectWithoutSuffix) shouldEqual subjectWithSuffix
    SchemaRegistryActor.addSchemaSuffix(subjectWithSuffix) shouldEqual subjectWithSuffix
  }

  "removeSchemaSuffix" should "remove the schema suffix if it exists" in {
    val subjectWithoutSuffix = "test.namespace-value.Test"
    val subjectWithSuffix = "test.namespace-value.Test-value"

    SchemaRegistryActor.removeSchemaSuffix(subjectWithoutSuffix) shouldEqual subjectWithoutSuffix
    SchemaRegistryActor.removeSchemaSuffix(subjectWithSuffix) shouldEqual subjectWithoutSuffix
  }
}

object SchemaRegistryActorSpec {
  val config =
    """
    akka {
      loglevel = "WARNING"
      actor {
        clustering = ClusterActorRefProvider
      }
    }
    """
}
