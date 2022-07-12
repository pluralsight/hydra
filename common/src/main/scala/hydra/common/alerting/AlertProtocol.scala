package hydra.common.alerting

import eu.timepit.refined.types.string
import hydra.common.util.TimeUtils
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonWriter, RootJsonFormat, enrichAny}

object AlertProtocol  extends DefaultJsonProtocol {


  case class NotificationScope(notificationLevel: NotificationLevel, notificationType: Option[NotificationType] = None)

  object NotificationScope {

    def apply[T <: NotificationLevel, K <: NotificationType](implicit level: T, notificationType: K): NotificationScope =
      NotificationScope(level, Some(notificationType))

  }

  case class StreamsNotification(message: String, level: String, stackTrace: JsValue,
                                 properties: Map[String, String], timestamp: String)

  object StreamsNotification {
    def make[T: JsonWriter](notificationMessage: NotificationMessage[T],
                            notificationInfo: NotificationScope,
                            properties: Map[String, String] = Map()): StreamsNotification = {
      def doCreateStreamNotification[D: JsonWriter](details: D): StreamsNotification = {
        new StreamsNotification(
          notificationMessage.message,
          notificationInfo.notificationLevel.toString,
          details.toJson,
          properties,
          TimeUtils.now()
        )
      }

      notificationMessage.notificationDetails match {
        case Some(details) => doCreateStreamNotification(details)
        case None => doCreateStreamNotification("")
      }
    }
  }

  case class NotificationMessage[T] private (message: String, notificationDetails: Option[T])

  object NotificationMessage {
    def apply(message: String): NotificationMessage[String] = NotificationMessage[String](message, None)
    def apply[T](message: String, details: T): NotificationMessage[T] =  NotificationMessage[T](message, Option(details))
  }

  case class NotificationRequest(notificationScope: NotificationScope, streamsNotification: StreamsNotification, url: Option[string.NonEmptyString])

  case class AuditSchemaReport(general: Option[Map[String, Seq[String]]], keySchema: Option[Map[String, Seq[String]]], valueSchema: Option[Map[String, Seq[String]]])

  implicit val AuditSchemaReportFormat: RootJsonFormat[AuditSchemaReport] = jsonFormat3(AuditSchemaReport)

  implicit val streamsFormat: RootJsonFormat[StreamsNotification] = jsonFormat5(StreamsNotification.apply)

}
