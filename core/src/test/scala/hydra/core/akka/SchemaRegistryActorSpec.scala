package hydra.core.akka

import java.net.ConnectException

import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor._
import hydra.core.protocol.HydraApplicationError
import org.apache.avro.Schema.Parser
import org.apache.avro.SchemaBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 3/9/17.
  */
class SchemaRegistryActorSpec
    extends TestKit(ActorSystem("SchemaRegistryActorSpec"))
    with Matchers
    with AnyFlatSpecLike
    with ImplicitSender
    with Eventually
    with BeforeAndAfterAll {

  implicit val timeout = Timeout(3.seconds)

  override def afterAll =
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10 seconds
    )

  val client = ConfluentSchemaRegistry.mockRegistry

  val testSchemaString = Source.fromResource("schema.avsc").mkString
  val testSchema = new Parser().parse(testSchemaString)

  val testKeySchema = SchemaBuilder
    .record("keySchema")
    .fields()
    .name("test")
    .`type`
    .stringType
    .noDefault
    .endRecord

  override def beforeAll() = {
    client.register("hydra.test.Tester-value", testSchema)
    client.register("hydra.test.TesterWithKey-key", testKeySchema)
    client.register("hydra.test.TesterWithKey-value", testSchema)
  }

  val listener = TestProbe()

  system.eventStream.subscribe(listener.ref, classOf[HydraApplicationError])

  val settings = new CircuitBreakerSettings(ConfigFactory.empty) {
    override val maxFailures = 2
    override val callTimeout = 1 second
    override val resetTimeout = 3 seconds
  }

  def fixture() = {
    val config = ConfigFactory.parseString("""
        |schema.registry.url = "mock"
        |http.port = 8080
        |http.interace = 0.0.0.0
      """.stripMargin)
    val schemaRegistryActor =
      system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    val testProbe = TestProbe()
    (testProbe, schemaRegistryActor)
  }

  "Receiving FetchSchemaRequest" should "open the circuit breaker when the registry is down" in {
    val config =
      ConfigFactory.parseString("""
        |schema.registry.url = "http://localhost:0101"
      """.stripMargin)
    val schemaRegistryActor =
      system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
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
      case FetchSchemaResponse(schema, keySchema) =>
        schema shouldBe SchemaResource(1, 1, testSchema)
        keySchema shouldBe empty
    }
    listener.expectNoMessage(3.seconds)
  }

  it should "respond with FetchSchemasResponse" in {
    val (probe, schemaRegistryActor) = fixture
    schemaRegistryActor.tell(FetchSchemasRequest(List("hydra.test.Tester", "hydra.test.TesterWithKey")), probe.ref)
    probe.expectMsgPF() {
      case FetchSchemasResponse(valueSchemas) =>
        valueSchemas shouldBe List(
          ("hydra.test.Tester", Option(SchemaResource(1, 1, testSchema))),
          ("hydra.test.TesterWithKey", Option(SchemaResource(1, 1, testSchema)))
        )
    }
  }

  it should "respond with FetchSchemaResponse with a key" in {
    val (probe, schemaRegistryActor) = fixture

    schemaRegistryActor.tell(
      FetchSchemaRequest("hydra.test.TesterWithKey"),
      probe.ref
    )
    probe.expectMsgPF() {
      case FetchSchemaResponse(schema, keySchema) =>
        schema shouldBe SchemaResource(1, 1, testSchema)
        keySchema shouldBe Some(SchemaResource(2, 1, testKeySchema))
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

  "Receiving RegisterSchemaResponse" should "save the schema metadata to the cache" in {
    val config =
      ConfigFactory.parseString("""
        |schema.registry.url = "http://localhost:0101"
      """.stripMargin)
    val schemaRegistryActor =
      system.actorOf(SchemaRegistryActor.props(config, Some(settings)))
    val senderProbe = TestProbe()

    val expectedResource = SchemaResource(1, 1, testSchema)
    schemaRegistryActor.tell(
      SchemaRegistered(1, 1, testSchemaString),
      senderProbe.ref
    )
    senderProbe.expectMsgPF() {
      case _ => {}
    }
    schemaRegistryActor.tell(
      FetchSchemaRequest("hydra.test.Tester"),
      senderProbe.ref
    )

    senderProbe.expectMsgPF() {
      case FetchSchemaResponse(actualSchema, keySchema) =>
        actualSchema shouldBe SchemaResource(1, 1, testSchema)
        keySchema shouldBe empty
    }
  }

  "The CircuitBreakerSettings" should "load settings from config" in {
    val cfg = ConfigFactory.parseString("""
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

  it should "fail registering the schema if hydra.key is a nullable field" in {
    val (probe, schemaRegistryActor) = fixture

    val invalid_schema =
      """
        |{
        |  "type": "record",
        |  "namespace": "exp.channels",
        |  "name": "ChannelCreated",
        |  "hydra.key": "description",
        |  "fields": [
        |    {
        |      "name": "description",
        |      "type": ["null", "string"],
        |      "default": null
        |    }
        |  ]
        |}
      """.stripMargin
    schemaRegistryActor.tell(RegisterSchemaRequest(invalid_schema), probe.ref)
    probe.expectMsgPF() {
      case e => e shouldBe a[Failure]
    }
  }

  it should "fail registering the schema if hydra.key isn't an existing field" in {
    val (probe, schemaRegistryActor) = fixture

    val invalid_schema =
      """
        |{
        |  "type": "record",
        |  "namespace": "exp.channels",
        |  "name": "ChannelCreated",
        |  "hydra.key": "non-existing",
        |  "fields": [
        |    {
        |      "name": "description",
        |      "type": "string",
        |      "default": null
        |    }
        |  ]
        |}
      """.stripMargin
    schemaRegistryActor.tell(RegisterSchemaRequest(invalid_schema), probe.ref)
    probe.expectMsgPF() {
      case e => e shouldBe a[Failure]
    }
  }

}
