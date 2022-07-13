package hydra.common.alerting


import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri
import cats.effect.IO
import cats.implicits._
import eu.timepit.refined.types.string.NonEmptyString
import hydra.common.IOSuite
import hydra.common.alerting.AlertProtocol.{NotificationMessage, NotificationScope, StreamsNotification}
import hydra.common.alerting.NotificationLevel._
import hydra.common.alerting.NotificationType.InternalNotification
import hydra.common.alerting.sender.NotificationSender
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{DefaultJsonProtocol, JsString, JsonWriter}

class NotificationSenderSpec extends AsyncFreeSpec with AsyncMockFactory with Matchers with IOSuite {

  import NotificationSenderSpec._

  "NotificationSenderSpec" - {

    "should send all types of notifications when only level is provided" in {
      val client = mock[NotificationsClient[IO]]

      setupCallsForClient(client, Set(NotificationType.InternalNotification), Set(Error))
      testAllNotificationsLevels(client)
    }

    "should send Debug, Info, Warn and Error notifications when level is Debug" in {
      val client = mock[NotificationsClient[IO]]
      val selectedNotifications: Set[NotificationType] = Set(InternalNotification)

      setupCallsForClient(client, selectedNotifications, Set(Debug, Info, Warn, Error))
      testAllNotificationsLevels(client)
    }

    "should send Info, Warn and Error notifications when level is Info" in {
      val client = mock[NotificationsClient[IO]]
      val selectedNotifications: Set[NotificationType] = Set(InternalNotification)

      setupCallsForClient(client, selectedNotifications, Set(Info, Warn, Error))
      testAllNotificationsLevels(client)
    }

    "should send Warn and Error notifications when level is Warn" in {
      val client = mock[NotificationsClient[IO]]
      val selectedNotifications: Set[NotificationType] = Set(InternalNotification)

      setupCallsForClient(client, selectedNotifications, Set(Warn, Error))
      testAllNotificationsLevels(client)
    }

    "should not send a notification when uri is not provided" in {
      val client = mock[NotificationsClient[IO]]
      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).never()

      testNotificationSender(client, NotificationType.InternalNotification, None, NotificationLevel.Error,
        NotificationMessage(messageText))
        .attempt
        .map(_.isRight shouldBe true)
    }

    "should not send a notification when uri cannot be parsed" in {
      val client = mock[NotificationsClient[IO]]
      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).never()

      testNotificationSender(client, NotificationType.InternalNotification, NonEmptyString.from(notValidUri).toOption,
        NotificationLevel.Error, NotificationMessage(messageText))
        .attempt
        .map(_.isRight shouldBe true)
    }

    "should not send a notification when properties cannot be parsed" in {
      val client = mock[NotificationsClient[IO]]
      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).never()

      testNotificationSender(client, NotificationType.InternalNotification, NonEmptyString.from(notValidUriProperty).toOption, NotificationLevel.Error, NotificationMessage(messageText))
        .attempt
        .map(_.isRight shouldBe true)
    }

    "should return InvalidUriProvided error when client throws an exception because of invalid uri" in {
      val client = mock[NotificationsClient[IO]]
      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).throws(NotificationsError.InvalidUriProvided(uri))

      testNotificationSender(client, NotificationType.InternalNotification, NonEmptyString.from(uri).toOption, NotificationLevel.Error, NotificationMessage(messageText))
        .attempt
        .map(_.isRight shouldBe true)
    }

    "should send a notification with given message and messageDetails" in {
      import spray.json._
      val client = mock[NotificationsClient[IO]]


      (client.postNotification(_: Uri, _: StreamsNotification)).expects(where {
        (_: Uri, notification: StreamsNotification) =>
          notification.message == messageText && notification.level == NotificationLevel.Info.toString && notification.stackTrace == messageDetailsText.toJson
      }).returns(IO.pure(JsString("")))

      testNotificationSender(client, NotificationType.InternalNotification, NonEmptyString.from(uri).toOption, NotificationLevel.Info, NotificationMessage(messageText, Some(messageDetailsText)))
        .attempt
        .map(_.isRight shouldBe true)
    }


    def setupCallsForClient(clientMock: NotificationsClient[IO],
                            notificationsShouldBeSent: Set[NotificationType],
                            levelsShouldBeSent: Set[NotificationLevel]): Unit = {
      for {
        level <- NotificationLevel.values
        notificationType = NotificationType.InternalNotification
      } yield {
        if (notificationsShouldBeSent.contains(notificationType) && levelsShouldBeSent.contains(level)) {
          (clientMock.postNotification(_: Uri, _: StreamsNotification)).expects(where {
            (_: Uri, notification: StreamsNotification) =>
              notification.message == messageText && notification.level == level.toString
          }).returns(IO.pure(JsString("")))
        } else {
          (clientMock.postNotification(_: Uri, _: StreamsNotification)).expects(where {
            (_: Uri, notification: StreamsNotification) =>
              notification.message == messageText && notification.level == level.toString
          }).never()
        }
      }
      ()
    }

    def testAllNotificationsLevels(client: NotificationsClient[IO]): IO[Assertion] = {
      val result = for {
        level <- NotificationLevel.values
      } yield testNotificationSender(client, NotificationType.InternalNotification, NonEmptyString.from(uri).toOption, level, NotificationMessage(messageText))

      result.toList.sequence.attempt.map(_.isRight shouldBe true)
    }

    def testNotificationSender[T: JsonWriter](client: NotificationsClient[IO],
                               notificationType: NotificationType,
                               destination: Option[NonEmptyString],
                               level: NotificationLevel,
                               notificationMessage: NotificationMessage[T]
                              ): IO[Unit] = {
      for {
        service <- NotificationSender[IO](client)
        _ <- service.send(NotificationScope(level, notificationType.some), notificationMessage, destination)
      } yield ()
    }
  }
}

object NotificationSenderSpec extends SprayJsonSupport with DefaultJsonProtocol {

  val messageText = "It's a test message."
  val messageDetailsText = "It's a detailed message."

  val uri = "test.com"
  val notValidUri = "notValid"
  val notValidUriProperty = "#"

}
