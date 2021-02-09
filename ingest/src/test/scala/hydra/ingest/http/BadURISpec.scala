package hydra.ingest.http

import akka.http.scaladsl.testkit.ScalatestRouteTest
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BadURISpec extends Matchers
  with AnyWordSpecLike
  with ScalatestRouteTest
  with HydraKafkaJsonSupport {

  private val badUriEndpoint = BadUri.route

  "The BadURI endpoint" should {
    "reject a bad URI" in {
      Get("//v2/topics") ~> badUriEndpoint ~> check {
        responseAs[String] shouldEqual("Unknown Path Provided: http://v2/topics")
      }
    }
  }

}
