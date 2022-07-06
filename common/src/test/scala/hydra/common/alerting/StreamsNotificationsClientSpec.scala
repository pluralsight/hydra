package hydra.common.alerting


import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO}
import hydra.common.alerting.AlertProtocol.StreamsNotification
import hydra.common.http.HttpRequestor
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import spray.json._

import scala.concurrent.Future
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class StreamsNotificationsClientSpec extends TestKit(ActorSystem("streams-notification-client-spec"))
  with AnyFlatSpecLike
  with Matchers
  with MockFactory
  with ScalaFutures {

  implicit val ec = system.dispatcher
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  val testNotification = StreamsNotification(
    "Ask Timeout on Actor [[Kafka]]",
    "ERROR",
    JsString("..."),
    properties = Map(
      "jobId" -> "kjf8sy978ryuohsdfljs",
      "applicationId" -> "role-iq-assessment-stage",
      "host" -> "hydra-streams-staging-1.vnerd.com",
      "userId" -> "roleiq"
    ),
    "2017-11-25T21:10:29.178+01:00"
  )

  "A StreamsNotificationClient" should "post a notification" in {
    val testUrl = "my-test-url.com?valOne=something"

    val request = HttpRequest(method = HttpMethods.POST, uri = Uri(testUrl),
      entity = testNotification.toJson.prettyPrint)

    val response = HttpResponse(entity = """"All good!"""")

    val httpRequestor = stub[HttpRequestor]

    (httpRequestor.makeRequest _)
      .when(request)
      .returns(Future.successful(response))

    (for {
      notificationsClient <- NotificationsClient.make(httpRequestor)
      response <- notificationsClient.postNotification(testUrl, testNotification)
    } yield {
      response shouldBe """"All good!"""".parseJson
      (httpRequestor.makeRequest _).verify(request).once
    }).unsafeRunSync()
  }
}

