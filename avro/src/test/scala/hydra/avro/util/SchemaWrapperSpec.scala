package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.scalatest.{FlatSpecLike, Matchers}

class SchemaWrapperSpec extends Matchers with FlatSpecLike {

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

    SchemaWrapper.from(avro).primaryKeys shouldBe Seq("id1","id2")

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

}
