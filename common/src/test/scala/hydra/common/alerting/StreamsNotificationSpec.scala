//package hydra.common.alerting
//
//
//import akka.http.scaladsl.model.Uri
//import cats.data.NonEmptyList
//import cats.effect.IO
//import cats.implicits._
//import eu.timepit.refined.refineV
//import eu.timepit.refined.types.string.NonEmptyString
//import hydra.common.IOSuite
//import org.scalamock.scalatest.AsyncMockFactory
//import org.scalatest.Assertion
//import org.scalatest.freespec.AsyncFreeSpec
//import org.scalatest.matchers.should.Matchers
//import spray.json._
//
//import java.time.Instant
//
//class StreamsNotificationsServiceSpec extends AsyncFreeSpec with AsyncMockFactory with Matchers with IOSuite {
//  import StreamsNotificationsServiceSpec._
//
//
//
//  "StreamsNotificationsServiceSpec" - {
//
//    "should send all levels of provided notification types" in {
//      val client = mock[NotificationsClient[IO]]
//      val selectedNotifications = Set(JobErrors, JobStopped)
//
//      setupCallsForClient(client, selectedNotifications, NotificationLevel.values)
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, selectedNotifications))
//    }
//
//    "should send all types of notifications when only level is provided" in {
//      val client = mock[NotificationsClient[IO]]
//
//      setupCallsForClient(client, NotificationType.jobNotifications, Set(Error))
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, notificationLevel = Error.some))
//    }
//
//    "should sent only provided notifications with certain level" in {
//      val client = mock[NotificationsClient[IO]]
//      val selectedNotifications = Set(JobStarted, JobDeleted)
//
//      setupCallsForClient(client, selectedNotifications, Set(Error))
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, selectedNotifications, Error.some))
//    }
//
//    "should send Debug, Info, Warn and Error notifications when level is Debug" in {
//      val client = mock[NotificationsClient[IO]]
//      val selectedNotifications = Set(JobStarted, JobDeleted)
//
//      setupCallsForClient(client, selectedNotifications, Set(Debug, Info, Warn, Error))
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, selectedNotifications, Debug.some))
//    }
//
//    "should send Info, Warn and Error notifications when level is Info" in {
//      val client = mock[NotificationsClient[IO]]
//      val selectedNotifications = Set(JobStarted, JobDeleted)
//
//      setupCallsForClient(client, selectedNotifications, Set(Info, Warn, Error))
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, selectedNotifications, Info.some))
//    }
//
//    "should send Warn and Error notifications when level is Warn" in {
//      val client = mock[NotificationsClient[IO]]
//      val selectedNotifications = Set(JobStarted, JobDeleted)
//
//      setupCallsForClient(client, selectedNotifications, Set(Warn, Error))
//      testAllJobNotifications(client, getKafkaReplicationDetails(uri.some, selectedNotifications, Warn.some))
//    }
//
//    "should not send a job notification when uri is not provided" in {
//      val client = mock[NotificationsClient[IO]]
//      (client.postNotification(_: Uri,_: StreamsNotification)).expects(*, *).never()
//
//      testJobNotification(client, getKafkaReplicationDetails(), JobStarted, Error)
//        .attempt
//        .map(_.isRight shouldBe true)
//    }
//
//    "should not send a job notification when uri cannot be parsed" in {
//      val client = mock[NotificationsClient[IO]]
//      (client.postNotification(_: Uri,_: StreamsNotification)).expects(*, *).never()
//
//      testJobNotification(client, getKafkaReplicationDetails(notValidUri.some), JobStarted, Error)
//        .attempt
//        .map(_.isRight shouldBe true)
//    }
//
//    "should not send a job notification when properties cannot be parsed" in {
//      val client = mock[NotificationsClient[IO]]
//      (client.postNotification(_: Uri,_: StreamsNotification)).expects(*, *).never()
//
//      val details = getKafkaReplicationDetails(notValidUriProperty.some)
//
//      testJobNotification(client, details, JobStarted, Error)
//        .attempt
//        .map(_.isRight shouldBe true)
//    }
//
//    "should return InvalidUriProvided error when client throws an exception because of invalid uri" in {
//      val client = mock[NotificationsClient[IO]]
//      (client.postNotification (_: Uri,_: StreamsNotification)).expects(*, *).throws(StreamsNotificationsError.InvalidUriProvided(uri))
//
//      testJobNotification(client, getKafkaReplicationDetails(uri.some), JobStarted, Error)
//        .attempt
//        .map(_.isRight shouldBe true)
//    }
////
////    "should send an audit job notification when uri is valid" in {
////      val client = mock[NotificationsClient[IO]]
////
////      (client.postNotification(_: Uri,_: StreamsNotification)).expects(where {
////        (_: Uri, notification: StreamsNotification) =>
////          notification.message == messageText && notification.level == "Error" && notification.stackTrace == messageDetailsText.toJson
////      }).returns(IO.pure(JsString("")))
////
////      testAuditNotification(client, getAuditJobDetails(uri.some), Error, messageText, messageDetailsText.some)
////        .attempt
////        .map(_.isRight shouldBe true)
////    }
////
////    "should not send an audit job notification when uri is not provided" in {
////      val client = mock[NotificationsClient[IO]]
////      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).never()
////
////      testAuditNotification(client, getAuditJobDetails(), Error, messageText, messageDetailsText.some)
////        .attempt
////        .map(_.isRight shouldBe true)
////    }
////
////    "should not send an audit job notification when uri cannot be parsed" in {
////      val client = mock[NotificationsClient[IO]]
////      (client.postNotification(_: Uri, _: StreamsNotification)).expects(*, *).never()
////
////      testAuditNotification(client, getAuditJobDetails(notValidUri.some), Error, messageText, messageDetailsText.some)
////        .attempt
////        .map(_.isRight shouldBe true)
////    }
////
////    "should send a notification to topic's owner if url was provided" in {
////      val client = mock[NotificationsClient[IO]]
////      val metadataAlgebra = mock[MetadataAlgebra[IO]]
////
////      (metadataAlgebra.getMetadataFor _).expects(*).returning(IO.pure(Some(topicMetadata(Some(uri)))))
////
////      (client.postNotification (_: Uri,_: StreamsNotification)).expects(where {
////        (_: Uri, notification: StreamsNotification) =>
////          notification.message == messageText && notification.level == NotificationLevel.Info.toString && notification.stackTrace == messageDetailsText.toJson
////      }).returns(IO.pure(JsString("")))
////
////      testNotificationForTopic(client, metadataAlgebra, getKafkaReplicationDetails(None, Set.empty, None), messageText, Some(messageDetailsText), NotificationType.NewSubscriber, NotificationLevel.Info)
////        .attempt
////        .map(_.isRight shouldBe true)
////    }
////
////    "should not send a notification to topic's owner if url was not provided" in {
////      val client = mock[NotificationsClient[IO]]
////      val metadataAlgebra = mock[MetadataAlgebra[IO]]
////
////      (metadataAlgebra.getMetadataFor _).expects(*).returning(IO.pure(Some(topicMetadata())))
////      (client.postNotification (_: Uri, _: StreamsNotification)).expects(*, *).never()
////
////      testNotificationForTopic(client, metadataAlgebra, getKafkaReplicationDetails(None, Set.empty, None), messageText, Some(messageDetailsText), NotificationType.NewSubscriber, NotificationLevel.Info)
////        .attempt
////        .map(_.isRight shouldBe true)
////    }
////
////    def setupCallsForClient(clientMock: NotificationsClient[IO],
////                            notificationsShouldBeSent: Set[NotificationType],
////                            levelsShouldBeSent: Set[NotificationLevel]): Unit = {
////      for {
////        notificationType <- NotificationType.values
////        level            <- NotificationLevel.values
////      } yield {
////        if (notificationsShouldBeSent.contains(notificationType) && levelsShouldBeSent.contains(level)) {
////          (clientMock.postNotification (_: Uri,_: StreamsNotification)).expects(where {
////            (_: Uri, notification: StreamsNotification) =>
////              notification.message == s"$notificationType$level" && notification.level == level.toString
////          }).returns(IO.pure(JsString("")))
////        } else {
////          (clientMock.postNotification (_: Uri,_: StreamsNotification)).expects(where {
////            (_: Uri, notification: StreamsNotification) =>
////              notification.message == s"$notificationType$level" && notification.level == level.toString
////          }).never()
////        }
////      }
////      ()
////    }
////
////    def testAllJobNotifications(client: NotificationsClient[IO], details: KafkaReplicationDetails): IO[Assertion] = {
////      val result = for {
////        notificationType <- NotificationType.jobNotifications
////        level            <- NotificationLevel.values
////      } yield testJobNotification(client, details, notificationType, level)
////
////      result.toList.sequence.attempt.map(_.isRight shouldBe true)
////    }
////
////    def testJobNotification(client: NotificationsClient[IO],
////                            details: KafkaReplicationDetails,
////                            notificationType: NotificationType,
////                            level: NotificationLevel): IO[Unit] =
////      for {
////        service <- NotificationSender[IO](client)
////        _       <- service.send(NotificationScope(level, notificationType),
////          NotificationMessage(s"$notificationType$level", messageDetailsText.some))(
////          JobReplicationData(jobId, details))
////      } yield ()
////
////    def testAuditNotification(client: NotificationsClient[IO],
////                              details: AuditJobDetails,
////                              level: NotificationLevel,
////                              message: String,
////                              messageDetails: Option[String]): IO[Unit] =
////      for {
////        service <- NotificationSender[IO](client)
////        _       <- service.send(NotificationScope(level), NotificationMessage(message, messageDetails))(details)
////      } yield ()
////
////
////    def testNotificationForTopic(client: NotificationsClient[IO],
////                                 metadataAlgebra: MetadataAlgebra[IO],
////                                 details: KafkaReplicationDetails,
////                                 message: String,
////                                 messageDetails: Option[String],
////                                 notificationType: NotificationType,
////                                 level: NotificationLevel): IO[Unit] = {
////      for {
////        service <- NotificationSender[IO](client)
////        _ <- service.send(NotificationScope(level, notificationType),
////          NotificationMessage(message, messageDetails))(
////          TopicOwnerNotificationData(topicName, jobId, details, subject => metadataAlgebra.getMetadataFor(subject))
////        )
////      } yield ()
////    }
////  }
////}
////object StreamsNotificationsServiceSpec {
////  val topicName = "dvs.test.topic"
////  val jobId = "someJobId"
////
////  val messageText        = "It's a test message."
////  val messageDetailsText = "It's a detailed message."
////
////  val uri                 = "test.com"
////  val notValidUri         = "тест"
////  val notValidUriProperty = "#"
////
////  def getKafkaReplicationDetails(notificationUrl: Option[String] = None, notificationTypes: Set[NotificationType] = Set.empty, notificationLevel: Option[NotificationLevel] = None): KafkaReplicationDetails = {
////    val notificationTypeProperty  = if (notificationTypes.isEmpty) "" else s", notifications: [${notificationTypes.mkString("," )}]"
////    val notificationLevelProperty = notificationLevel.fold("")(level => s", notificationsLevel: $level")
////    val notificationUrlProperty   = notificationUrl.fold("")(url => s"notificationsUrl: $url")
//
//    KafkaReplicationDetails.make(
//      jobName = "someJob",
//      consumerGroupName = "appId",
//      Right(""),
//      startingOffsets = "latest",
//      Map.empty,
//      s"{$notificationUrlProperty $notificationTypeProperty $notificationLevelProperty}",
//      DatabaseConnectionInfo("", None, None)
//    )
//  }
//
//  def getAuditJobDetails(notificationUrl: Option[String] = None): AuditJobDetails =
//    AuditJobDetails(
//      refineV[ConsumerGroupNameRegex](s"auditTestConsumer").toOption.get,
//      StartingOffsets.Earliest,
//      notificationUrl.map(NonEmptyString.unsafeFrom),
//      V2SourceInfo("topicName", RecordFormats(DataFormat.Avro, DataFormat.Avro), None, List.empty),
//      none,
//      List.empty,
//      Map.empty,
//      AuditType.LogicalTypeCheck
//    )
//
//  def topicMetadata(notificationUrl: Option[String] = None) = TopicMetadataContainer(
//    TopicMetadataV2Key(Subject.createValidated(topicName).get),
//    TopicMetadataV2Value(
//      StreamTypeV2.Entity,
//      deprecated = false,
//      None,
//      Public,
//      NonEmptyList.one(Slack.create("#channel").get),
//      Instant.now,
//      List(),
//      None,
//      Some("dvs-teamName"),
//      List.empty,
//      notificationUrl
//    ),
//    None, None
//  )
//}
