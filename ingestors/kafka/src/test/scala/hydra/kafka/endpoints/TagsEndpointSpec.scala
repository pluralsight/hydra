package hydra.kafka.endpoints

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.ContentNegotiator.Alternative.ContentType
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.kafka.algebras.{KafkaClientAlgebra, TagsAlgebra}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext

class TagsEndpointSpec extends Matchers with AnyWordSpecLike with ScalatestRouteTest{

  implicit val syncit = Sync[IO]
  implicit private val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
  private implicit val concurrentEffect: Concurrent[IO] = IO.ioConcurrentEffect
  private implicit val catsLogger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLogger[IO]

  val schemaRegistry = SchemaRegistry.test[IO].unsafeRunSync()
  val kafkaClientAlgebra = KafkaClientAlgebra.test[IO](schemaRegistry).unsafeRunSync()

  val tagsAlgebra = TagsAlgebra.make("dvs.topic","someClient", kafkaClientAlgebra).unsafeRunSync()
  val route = new TagsEndpoint[IO](tagsAlgebra, "myPass", "tagsTopic", kafkaClientAlgebra).route

  val validCredentials = BasicHttpCredentials("John", "myPass")


  "The TagsEndpoint path" should {
    "create a new tag" in {
      Post("/v2/tags", HttpEntity(ContentTypes.`application/json`, """{"name": "DVS", "description": "Created by DVS"}""")) ~>
        addCredentials(validCredentials) ~> Route.seal(route) ~> check {
        status shouldBe StatusCodes.OK
      }
    }
    "get all tags in the topic" in {
      Get("/v2/tags", HttpEntity(ContentTypes.`application/json`, "")) ~>
        Route.seal(route) ~> check {
        responseAs[String] shouldBe """[{"name": "DVS", "description": "Created by DVS"}]"""
        status shouldBe StatusCodes.OK
      }
    }
  }

}
