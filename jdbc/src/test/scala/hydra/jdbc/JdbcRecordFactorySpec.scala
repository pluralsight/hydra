package hydra.jdbc

import java.io.InputStream

import akka.actor.{ Actor, ActorSystem, Props }
import akka.testkit.TestKit
import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor.{ FetchSchemaRequest, FetchSchemaResponse }
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.protocol.MissingMetadataException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FunSpecLike, Matchers }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

class JdbcRecordFactorySpec extends TestKit(ActorSystem("hydra"))
  with Matchers
  with FunSpecLike
  with ScalaFutures {

  val schemaPK = new Schema.Parser().parse(Source.fromResource("schemaPK.avsc").mkString)
  val schemaNPK = new Schema.Parser().parse(Source.fromResource("jdbc-test.avsc").mkString)

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(2000 millis),
    interval = scaled(100 millis))

  def schemaResource(schemaToUse: Schema) = new SchemaResource {
    override def schema: Schema = schemaToUse

    override def id: Int = 1

    override def version: Int = 1

    override def location: String = "location"

    override def getDescription: String = "description"

    override def getInputStream: InputStream = null //not testing this
  }

  val loader = system.actorOf(Props(new Actor() {
    override def receive: Receive = {
      case FetchSchemaRequest(schema) =>
        val schemaToUse = if (schema == "classpath:schemaPK.avsc") schemaPK else schemaNPK
        sender ! FetchSchemaResponse(schemaResource(schemaToUse))
    }
  }))

  val factory = new JdbcRecordFactory(loader)

  describe("The JDBC record factory") {
    it("extracts primary keys if present") {
      val req = HydraRequest("1", "test")
      JdbcRecordFactory.pk(req, schemaPK) shouldBe Seq(schemaPK.getField("id"))

      val req1 = HydraRequest("1", "test")
      JdbcRecordFactory.pk(req1, schemaNPK) shouldBe Seq.empty

      val req2 = HydraRequest("1", "test").withMetadata(JdbcRecordFactory.PRIMARY_KEY_PARAM -> "id")
      JdbcRecordFactory.pk(req2, schemaNPK) shouldBe Seq(schemaNPK.getField("id"))

      val req3 = HydraRequest("1", "test").withMetadata(JdbcRecordFactory.PRIMARY_KEY_PARAM -> "unknown")
      intercept[IllegalArgumentException] {
        JdbcRecordFactory.pk(req3, schemaNPK) shouldBe Seq(schemaNPK.getField("id"))
      }
    }

    it("throws an error if no schema is in the request metadata") {
      val req = HydraRequest("1", "test")
      whenReady(factory.build(req).failed)(_ shouldBe an[MissingMetadataException])
    }

    it("throws an error if payload does not comply to schema") {
      val request = HydraRequest("123", """{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:jdbc-test.avsc")
      whenReady(factory.build(request)
        .failed)(_ shouldBe a[JsonToAvroConversionExceptionWithMetadata])
    }

    it("throws an error if payload if validation is strict") {
      val request = HydraRequest("123", """{"id":1, "field":2, "name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:jdbc-test.avsc")
      whenReady(factory.build(request)
        .failed)(_ shouldBe a[JsonToAvroConversionExceptionWithMetadata])
    }

    it("Uses the schema as the table name") {
      val request = HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:jdbc-test.avsc", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      whenReady(factory.build(request))(_.destination shouldBe schemaNPK.getName)

    }

    it("throws an error if no db profile is present in the request") {
      val request = HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:jdbc-test.avsc", JdbcRecordFactory.TABLE_PARAM -> "table")
      whenReady(factory.build(request)
        .failed)(_ shouldBe an[MissingMetadataException])
    }

    it("builds a record without a PK") {
      val request = HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(
          HYDRA_SCHEMA_PARAM -> "classpath:jdbc-test.avsc",
          JdbcRecordFactory.TABLE_PARAM -> "table", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      whenReady(factory.build(request)) { rec =>
        rec.destination shouldBe "table"
        rec.key shouldBe Some(Seq.empty)
        rec.payload shouldBe new JsonConverter[GenericRecord](schemaNPK).convert("""{"id":1, "name":"test", "rank" : 1}""")
      }
    }

    it("builds a record with a PK") {
      val request = HydraRequest("123", """{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schemaPK.avsc", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      whenReady(factory.build(request)) { rec =>
        rec.destination shouldBe schemaPK.getName
        rec.key shouldBe Some(Seq(schemaPK.getField("id")))
        rec.payload shouldBe new JsonConverter[GenericRecord](schemaPK).convert("""{"id":1, "name":"test", "rank" : 1}""")
      }
    }
  }
}
