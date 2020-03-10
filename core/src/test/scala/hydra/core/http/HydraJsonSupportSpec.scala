package hydra.core.http

import java.util.UUID

import akka.actor.ActorPath
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.marshallers._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.util.{Success, Try}

class HydraJsonSupportSpec
    extends Matchers
    with AnyFunSpecLike
    with HydraJsonSupport
    with SprayJsonSupport {

  import spray.json._

  describe("Hydra Json Support") {
    it("converts StatusCodes") {
      val s = StatusCodes.OK.asInstanceOf[StatusCode]
      s.toJson shouldBe """{"code":200,"message":"OK"}""".parseJson
      intercept[NotImplementedError] {
        """{"code":200,"message":"OK"}""".parseJson.convertTo[StatusCode]
      }
    }

    it("converts ActorPath objects") {
      val s = ActorPath.fromString("akka://hydra/user/test")
      s.toJson shouldBe JsString("akka://hydra/user/test")
      """"akka://hydra/user/test"""".parseJson.convertTo[ActorPath] shouldBe s
    }

    it("converts Throwable objects") {
      val s = new IllegalArgumentException("error")
      s.asInstanceOf[Throwable]
        .toJson
        .toString
        .indexOf(
          """"message":"error","stackTrace":"java.lang.IllegalArgumentException"""
        ) should not be -1
      intercept[NotImplementedError] {
        """"error"""".parseJson.convertTo[Throwable]
      }
    }

    it("converts Try objects") {
      val s = Success("DONE!")
      s.asInstanceOf[Try[String]]
        .toJson
        .toString shouldBe """{"success":"DONE!"}"""
      val z = scala.util.Failure(new RuntimeException("ERROR"))
      z.asInstanceOf[Try[String]]
        .toJson
        .toString
        .indexOf(
          """"message":"ERROR","stackTrace":"java.lang.RuntimeException"""
        ) should not be -1
    }

    it("converts UUID objects") {
      val s = UUID.randomUUID()
      s.toJson shouldBe JsString(s.toString)
      s""""${s.toString}"""".parseJson.convertTo[UUID] shouldBe s
      intercept[DeserializationException] {
        s""""123"""".parseJson.convertTo[UUID]
      }
      intercept[DeserializationException] {
        JsNumber(1).convertTo[UUID]
      }
    }

    it("converts DateTime objects") {
      val formatter = ISODateTimeFormat.basicDateTimeNoMillis()
      val s = DateTime.now.withMillis(0)
      s.toJson shouldBe JsString(s.toString(formatter))
      s""""${s.toString(formatter)}"""".parseJson.convertTo[DateTime] shouldBe s
      intercept[DeserializationException] {
        s""""123"""".parseJson.convertTo[DateTime] shouldBe s
      }
      intercept[DeserializationException] {
        JsNumber(1).convertTo[DateTime]
      }
    }

    it("converts StreamType objects") {

      val hist = JsString("History")
      hist.convertTo[StreamType] shouldBe History
      val curr = JsString("CurrentState")
      curr.convertTo[StreamType] shouldBe CurrentState
      val notf = JsString("Notification")
      notf.convertTo[StreamType] shouldBe Notification
      val tel = JsString("Telemetry")
      tel.convertTo[StreamType] shouldBe Telemetry

      intercept[DeserializationException] {
        JsString("dummy").convertTo[StreamType] shouldBe History
      }

    }
  }
}
