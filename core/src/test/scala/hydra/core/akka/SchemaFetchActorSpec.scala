package hydra.core.akka

import java.io.File

import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.{ConfluentSchemaRegistry, SchemaRegistryException}
import hydra.core.akka.SchemaFetchActor.{FetchSchema, SchemaFetchResponse}
import hydra.core.protocol.HydraApplicationError
import org.apache.avro.Schema.Parser
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class SchemaFetchActorSpec extends TestKit(ActorSystem("hydra"))
  with Matchers
  with FlatSpecLike
  with ImplicitSender
  with Eventually
  with BeforeAndAfterAll {

  override def afterAll = TestKit.shutdownActorSystem(system, verifySystemShutdown = true,
    duration = 10 seconds)

  val client = ConfluentSchemaRegistry.mockRegistry

  val testSchema = Thread.currentThread().getContextClassLoader
    .getResource("schema.avsc").getFile

  override def beforeAll() = {
    client.register("hydra.test.Tester-value", new Parser().parse(new File(testSchema)))
  }

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[HydraApplicationError])

  val settings = new CircuitBreakerSettings(ConfigFactory.empty) {
    override val maxFailures = 2
    override val callTimeout = 1 second
    override val resetTimeout = 3 seconds
  }

  val config = ConfigFactory.parseString(
    """
      |schema.registry.url = "http://localhost:0101"
    """.stripMargin)


  "The schema fetcher" should "open the circuit breaker" in {
    val fetcher = system.actorOf(SchemaFetchActor.props(config, Some(settings)))
    fetcher ! FetchSchema("test")
    fetcher ! FetchSchema("test")
    fetcher ! FetchSchema("test")
    listener.expectMsgType[HydraApplicationError]
    expectMsgPF() {
      case Failure(ex) => ex shouldBe a[SchemaRegistryException]
    }
  }

  it should "not open the circuit for looking up schemas that don't exist" in {
    val probe = TestProbe()
    val cfg = ConfigFactory.parseString(
      """
        |schema.registry.url = mock
      """.stripMargin)

    val fetcher = system.actorOf(SchemaFetchActor.props(cfg, Some(settings)))
    fetcher.tell(FetchSchema("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    fetcher.tell(FetchSchema("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    fetcher.tell(FetchSchema("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    fetcher.tell(FetchSchema("unknown"), probe.ref)
    probe.expectMsgType[Failure]
    listener.expectNoMessage(3.seconds)
  }

  it should "send a valid response" in {
    val probe = TestProbe()
    val cfg = ConfigFactory.parseString(
      """
        |schema.registry.url = mock
      """.stripMargin)

    val fetcher = system.actorOf(SchemaFetchActor.props(cfg, Some(settings)))
    fetcher.tell(FetchSchema("hydra.test.Tester"), probe.ref)
    probe.expectMsgPF() {
      case SchemaFetchResponse(resource) =>
        resource.schema shouldBe new Parser().parse(new File(testSchema))
    }
    listener.expectNoMessage(3.seconds)
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

}