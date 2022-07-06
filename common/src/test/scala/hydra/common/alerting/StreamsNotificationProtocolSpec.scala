package hydra.common.alerting

import hydra.common.alerting.AlertProtocol.StreamsNotification
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StreamsNotificationProtocolSpec extends AnyFlatSpec
  with Matchers {

  import spray.json._

  val streamsAlert = StreamsNotification(
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

  "StreamsAlertFormat" should "create a json payload" in {
    val expected = JsObject(
      "timestamp" -> JsString("2017-11-25T21:10:29.178+01:00"),
      "message" -> JsString("Ask Timeout on Actor [[Kafka]]"),
      "level" -> JsString("ERROR"),
      "stackTrace" -> JsString("..."),
      "properties" -> JsObject(Map(
        "jobId" -> JsString("kjf8sy978ryuohsdfljs"),
        "applicationId" -> JsString("role-iq-assessment-stage"),
        "host" -> JsString("hydra-streams-staging-1.vnerd.com"),
        "userId" -> JsString("roleiq")
      ))
    )

    val jf = implicitly[RootJsonFormat[StreamsNotification]]

    jf.write(streamsAlert) shouldEqual expected
  }

  it should "read a json payload and convert it to a StreamsAlert" in {
    val jsonAlert =
      """
        |{
        |    "timestamp": "2017-11-25T21:10:29.178+01:00",
        |    "message": "Ask Timeout on Actor [[Kafka]]",
        |    "level": "ERROR",
        |    "stackTrace": "...",
        |    "properties": {
        |        "jobId": "kjf8sy978ryuohsdfljs",
        |        "applicationId": "role-iq-assessment-stage",
        |        "host": "hydra-streams-staging-1.vnerd.com",
        |        "userId": "roleiq"
        |    }
        |}
      """.stripMargin

    val parsedAlert = jsonAlert.parseJson.convertTo[StreamsNotification]

    parsedAlert shouldEqual streamsAlert
  }
}
