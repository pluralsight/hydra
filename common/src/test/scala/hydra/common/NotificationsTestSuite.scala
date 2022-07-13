package hydra.common

import cats.Monad
import hydra.common.alerting.AlertProtocol.{NotificationMessage, NotificationScope}
import hydra.common.alerting.NotificationRequestBuilder
import hydra.common.alerting.sender.{InternalNotificationSender, NotificationSender}
import org.scalamock.scalatest.MockFactory
import spray.json.JsonFormat

trait NotificationsTestSuite extends MockFactory {

  def getNotificationSenderMock[F[_] : Monad]: NotificationSender[F] = {
    val notificationSenderStub = stub[NotificationSender[F]]

    (notificationSenderStub.send(_: NotificationScope, _: NotificationMessage[String], _: Any)(_: JsonFormat[String], _: NotificationRequestBuilder[F, Any]))
      .when(*, *, *, *, *)
      .returns(Monad[F].unit)

    notificationSenderStub
  }

  def getInternalNotificationSenderMock[F[_] : Monad] = new InternalNotificationSender[F](None, getNotificationSenderMock[F])

}
