package hydra.core.ingest

import org.scalatest.{FunSpecLike, Matchers}
import spray.json.DefaultJsonProtocol

/**
  * Created by alexsilva on 3/22/17.
  */
class RequestUtilsSpec extends Matchers with FunSpecLike with DefaultJsonProtocol {

  import spray.json._

  val json =
    """ [
      |        { "name":"Ford", "models":[ "Fiesta", "Focus", "Mustang" ] },
      |        { "name":"BMW", "models":[ "320", "X3", "X5" ] },
      |        { "name":"Fiat", "models":[ "500", "Panda" ] }
      |    ]
    """.stripMargin

  val jsArray = json.parseJson.convertTo[JsArray].elements.map(_.compactPrint)
  describe("When using RequestUtils") {
    it("Splits a json array request") {
      val r = HydraRequest(payload = json)
      RequestUtils.split(r).map(_.payload) shouldBe jsArray
    }
  }

}