package hydra.core.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.GenericHttpCredentials
import org.scalatest.{FlatSpec, MustMatchers}

class HydraAuthenticationClientSpec extends FlatSpec
  with MustMatchers {

  implicit val sys = ActorSystem("hydra-auth-client")

  "Authenticating with a valid token" must
    "return the user's name" in {
//    val client = new HydraAuthenticationClient

    val creds = GenericHttpCredentials("Bearer", "12345")

//    client.auth(Some(creds)) mustBe Some("john.doe")
  }

  "Authenticating without credentials" must
    "return None" in {
    fail()
  }

  "HydraAuthenticationClient" must
    "build a request from a token" in {
    fail()
  }

  it must
    "parse the name from a JSON response" in {
    fail()
  }
}
