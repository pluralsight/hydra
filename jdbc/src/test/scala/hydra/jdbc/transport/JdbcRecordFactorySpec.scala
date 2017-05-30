package hydra.jdbc.transport

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.avro.schema.GenericSchemaResource
import hydra.core.ingest.{HydraRequest, HydraRequestMetadata, RequestParams}
import hydra.core.transport.RetryStrategy
import org.apache.avro.generic.GenericRecord
import org.scalatest.{Matchers, WordSpecLike}
import org.springframework.core.io.ClassPathResource

/**
  * Created by alexsilva on 5/19/17.
  */
class JdbcRecordFactorySpec extends Matchers with WordSpecLike {

  val req = HydraRequest(payload = """{"id":1,"username":"test"}""",
    metadata = Seq(HydraRequestMetadata(RequestParams.HYDRA_SCHEMA_PARAM, "classpath:avro.avsc")))

  "The jdbc record factory" should {
    "extract the table name from the schema" in {
      assert(JdbcRecordFactory.build(req).get.destination == "User")
    }

    "use table name if supplied as metadata" in {
      assert(JdbcRecordFactory.build(req.withMetadata("hydra-jdbc-table" -> "TheTable"))
        .get.destination == "TheTable")
    }

    "fail if no schema" in {
      val rec = JdbcRecordFactory.build(HydraRequest(payload = """{"id":1,"username":"test"}"""))
      rec.isFailure shouldBe true
    }

    "create the record with no primary key" in {
      val rec = JdbcRecordFactory.build(req.withMetadata("hydra-jdbc-table" -> "TheTable"))
      val schema = new GenericSchemaResource(new ClassPathResource("avro.avsc"))
      rec.foreach { r =>
        val converted = new JsonConverter[GenericRecord](schema.schema).convert("""{"id":1,"username":"test"}""")
        r.destination shouldBe "TheTable"
        r.payload shouldBe converted
        r.key shouldBe None
        r.retryStrategy shouldBe RetryStrategy.Fail
      }
    }

    "create the record with a primary key" in {
      val rec = JdbcRecordFactory.build(req.withMetadata("hydra-jdbc-primary-key" -> "id"))
      val schema = new GenericSchemaResource(new ClassPathResource("avro.avsc"))
      rec.foreach { r =>
        r.destination shouldBe "User"
        r.payload shouldBe new JsonConverter[GenericRecord](schema.schema).convert("""{"id":1,"username":"test"}""")
        r.key shouldBe Some("id")
        r.retryStrategy shouldBe RetryStrategy.Fail
      }
    }
  }
}
