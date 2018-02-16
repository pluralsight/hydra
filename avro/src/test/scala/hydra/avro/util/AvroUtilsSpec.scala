package hydra.avro.util

import java.io.InputStream

import com.pluralsight.hydra.avro.RequiredFieldMissingException
import hydra.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.avro.util.AvroUtils.SeenPair
import org.apache.avro.Schema
<<<<<<< HEAD
import org.scalatest.{FunSpecLike, Matchers}
=======
import org.apache.avro.Schema.Field
import org.scalatest.{ FunSpecLike, Matchers }
>>>>>>> remove classpath functionality for schema registry actor

/**
 * Created by alexsilva on 7/6/17.
 */
class AvroUtilsSpec extends Matchers with FunSpecLike {

  describe("When using AvroUtils") {
    it("replaces invalid characters") {
      AvroUtils.cleanName("!test") shouldBe "_test"
      AvroUtils.cleanName("?test") shouldBe "_test"
      AvroUtils.cleanName("_test") shouldBe "_test"
      AvroUtils.cleanName("test") shouldBe "test"
      AvroUtils.cleanName("1test") shouldBe "1test"
    }

    it("Gets a schema field by name") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [
          |		{
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)

      AvroUtils.getField("testEnum", avro) shouldBe avro.getField("testEnum")
      intercept[IllegalArgumentException] {
        AvroUtils.getField("unknown", avro)
      }
    }


    it("tests for equality ignoring props") {
      val schema1 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      val schema2 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      AvroUtils.areEqual(schema1, schema2) shouldBe true
    }

    it("returns false for schemas with different names") {
      val schema1 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra1",
          | "hydra.key": "id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      val schema2 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      AvroUtils.areEqual(schema1, schema2) shouldBe false

    }

    it("returns false for schemas with same fields but different doc tags") {

      val schema1 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin)

      val schema2 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		}
          |	]
          |}""".stripMargin)

      AvroUtils.areEqual(schema1, schema2) shouldBe true

    }

    it("returns false for schemas with different fields") {
      val schema1 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin)

      val schema2 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		}
          |	]
          |}""".stripMargin)

      AvroUtils.areEqual(schema1, schema2) shouldBe false

    }

    it("uses the equals cache") {
      val schema1 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin)

      val schema2 = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		}
          |	]
          |}""".stripMargin)

      AvroUtils.areEqual(schema1, schema2) shouldBe true
      AvroUtils.SEEN_EQUALS.get().contains(SeenPair(schema1.hashCode(), schema2.hashCode())) shouldBe true

      AvroUtils.areEqual(schema1, schema2) shouldBe true

    }

    it("improves exception") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [
          |		{
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin

      val tschema = new Schema.Parser().parse(schema)

      AvroUtils.improveException(
        new IllegalArgumentException(""),
        tschema) shouldBe an[IllegalArgumentException]

      val ex = new RequiredFieldMissingException("testEnum", tschema)
      val improved = AvroUtils.improveException(ex, tschema)
      improved shouldBe an[JsonToAvroConversionExceptionWithMetadata]
      val iex = improved.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
      iex.getMessage should not be null
      iex.cause shouldBe a[RequiredFieldMissingException]
      iex.res shouldBe tschema

    }
  }
}
