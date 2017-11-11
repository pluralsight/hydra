package hydra.core.protocol

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.Matchers
import org.scalatest.FlatSpecLike

class IngestorStatusSpec extends Matchers with FlatSpecLike {

  it should "return the right status codes" in {
    val ex = new IllegalArgumentException("")
    IngestorTimeout.statusCode.intValue shouldBe 408
    IngestorTimeout.completed shouldBe true
    IngestorJoined.statusCode.intValue shouldBe 202
    IngestorJoined.completed shouldBe false
    IngestorIgnored.statusCode.intValue shouldBe 406
    IngestorIgnored.completed shouldBe false
    RequestPublished.statusCode.intValue shouldBe 201
    RequestPublished.completed shouldBe false
    InvalidRequest(ex).statusCode.intValue shouldBe 400
    InvalidRequest(ex).completed shouldBe true
    IngestorError(0, ex).statusCode.intValue shouldBe 503
    IngestorError(0, ex).completed shouldBe true
    IngestorCompleted.statusCode.intValue shouldBe 200
    IngestorCompleted.completed shouldBe true
    IngestorCompleted.message shouldBe StatusCodes.OK.reason
  }

  it should "create an invalid request from a string" in {
    new InvalidRequest("error!").error shouldBe a[IllegalArgumentException]
    new InvalidRequest("error!").error.getMessage shouldBe "error!"
  }
}
