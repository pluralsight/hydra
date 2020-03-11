package hydra.sql

import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class BufferedRecordsSpec extends Matchers with AnyFunSpecLike {

  val schema = new Schema.Parser().parse("""
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      | "key":"id",
      |
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |		{
      |			"name": "active",
      |			"type": "boolean"
      |		}
      |	]
      |}
    """.stripMargin)

  describe("The BufferedRecords class") {}

}
