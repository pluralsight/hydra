package hydra.ingest.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, TestConsumerGroupsAlgebra}
import hydra.kafka.model.TopicConsumer.{TopicConsumerKey, TopicConsumerValue}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class HealthEndpointSpec
    extends Matchers
    with AnyFlatSpecLike
    with ScalatestRouteTest {

  val uniqueName = TestConsumerGroupsAlgebra.empty.getUniquePerNodeConsumerGroup
  val uNK = TopicConsumerKey("someTopic", uniqueName)
  val uNV = TopicConsumerValue(Instant.now())


  "The HealthEndpoint" should "return a 200" in {
    val testCGA = TestConsumerGroupsAlgebra.empty.addConsumerGroup(uNK, uNV, "Stable")

    Get("/health") ~> new HealthEndpoint[IO](testCGA).route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  "The HealthEndpoint" should "return a 500" in {
    val testCGA = TestConsumerGroupsAlgebra.empty.addConsumerGroup(uNK, uNV, "Empty")

    Get("/health") ~> new HealthEndpoint[IO](testCGA).route ~> check {
      status shouldEqual StatusCodes.InternalServerError
    }
  }
}
