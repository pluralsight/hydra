package hydra.sql

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class BufferedRecordsSpec extends Matchers with FunSpecLike {

  val schema = new Schema.Parser().parse(
    """
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

  describe("The BufferedRecords class") {



  }

}
