package hydra.core.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.Authorization
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import hydra.core.auth.HydraAuthenticationClient.{MissingCredentialsException, extractUserName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class HydraAuthenticationClientSpec extends TestKit(ActorSystem("hydra-auth-client"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  implicit val mat = ActorMaterializer()

  implicit val ec = system.dispatcher

  val payload =
    """
      |{
      |	"id": 1,
      |	"username": "john.doe",
      |	"firstName": "John",
      |	"lastName": "Doe",
      |	"roles": [{
      |		"id": 1,
      |		"roleName": "STANDARD_USER",
      |		"description": "Standard User - Has no admin rights"
      |	}]
      |}
    """.stripMargin


  "A HydraAuthenticationClient" should
    "return the user name on a valid request" in {
    fail()
  }

  it should "return a `MissingCredentialsException` if credentials are not provided" in {
    val client = new HydraAuthenticationClient()

    whenReady(client.auth(None).failed) { e =>
      e shouldBe a[MissingCredentialsException]
    }
  }

  import HydraAuthenticationClient._

  it should "return an `AuthenticationFailureException` on receipt of a failed response" in {
    val failedResponse = HttpResponse(StatusCodes.BadRequest)

    val failedFuture = extractUserName(failedResponse)

    whenReady(failedFuture.failed) { e =>
      e shouldBe an[AuthenticationFailureException]
    }
  }

  it should "build a request from a token" in {
    val token = "token1234"

    val request = buildRequest(token)

    request.method shouldEqual HttpMethods.POST
    request.uri.toString.endsWith("hydra-auth/whoami") shouldBe true
    request.headers.head shouldBe a[Authorization]
  }

  it should "parse the username from a json payload" in {
    val testResponse = HttpResponse(entity = payload)

    extractUserName(testResponse).map { userName =>
      userName shouldBe "john.does"
    }
  }
}
