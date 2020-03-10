package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Parser}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class SchemaWrapperSpec extends Matchers with AnyFlatSpecLike {

  "The schema wrapper" should "allow overriding of primary keys via the method argument" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "username",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro).primaryKeys(0) shouldBe "username"
    SchemaWrapper.from(avro, Seq("id")).primaryKeys(0) shouldBe "id"

  }

  it should "return a single primary key" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "id",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro).primaryKeys shouldBe Seq("id")
  }

  it should "throw an error when a primary key is invalid" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "someKey",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    intercept[IllegalArgumentException] {
      SchemaWrapper.from(avro).validate().get
    }
  }

  it should "validate" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "id",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": "string"
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro).validate().get

  }

  it should "validate primary key fields are not nullable" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "username",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    intercept[IllegalArgumentException] {
      SchemaWrapper.from(avro).validate().get
    }

  }

  it should "allow primary keys to be supplied" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        |	"fields": [{
        |			"name": "id",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro, Seq.empty).primaryKeys shouldBe Seq.empty[Field]
  }

  it should "return a composite primary key" in {
    val schema =
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
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro).primaryKeys shouldBe Seq("id1", "id2")

  }

  it should "strip whitespace from a composite primary key" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "id1, id2",
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
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro).primaryKeys shouldBe Seq("id1", "id2")
  }

  it should "allow empty primary keys" in {
    val schema =
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "hydra.key": "id1, id2",
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
        |}""".stripMargin

    val avro = new Schema.Parser().parse(schema)

    SchemaWrapper.from(avro, Seq.empty).primaryKeys shouldBe Seq.empty
  }

  it should "reject a schema when a primary key given does not exist" in {
    val schema =
      """
        |{
        |    "type": "record",
        |    "name": "User",
        |    "namespace": "hydra",
        | "hydra.key": "doesnotexist",
        |    "fields": [{
        |            "name": "id",
        |            "type": "int"
        |        },
        |        {
        |            "name": "username",
        |            "type": "string"
        |        }
        |    ]
        |}
      """.stripMargin
    val testSchema = new Parser().parse(schema)
    intercept[IllegalArgumentException] {
      SchemaWrapper.from(testSchema).validate().get
    }
  }

}
