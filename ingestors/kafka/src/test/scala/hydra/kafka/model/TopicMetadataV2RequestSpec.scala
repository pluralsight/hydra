package hydra.kafka.model

import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.scalatest.{Matchers, WordSpec}

class TopicMetadataV2RequestSpec extends WordSpec with Matchers{

  "TopicMetadataV2Request" must {

    "Parse correct emails" in {
      val correctEmailAddress = "data-platform-team@pluralsight.com"
      val correctEmail = ContactMethod.Email.create(correctEmailAddress)
      correctEmail.get shouldBe a[ContactMethod.Email]
      correctEmail.get.address.value shouldEqual(correctEmailAddress)
    }

    "Return None for incorrect emails" in {
      ContactMethod.Email.create("data-platform-team@pluralsight") shouldBe None
    }

    "Parse correct slacks" in {
      val correctSlackChannel = "#dev-data-platform"
      val correctSlack = ContactMethod.Slack.create(correctSlackChannel)
      correctSlack.get shouldBe a[ContactMethod.Slack]
      correctSlack.get.channel.value shouldEqual(correctSlackChannel)
    }

    "Return None for incorrect slacks" in {
      ContactMethod.Slack.create("Not a slack address") shouldBe None
    }

    "Parse correct Subjects" in {
      val correctSubjectString = "Foo"
      val correctSubject = Subject.createValidated(correctSubjectString)
      correctSubject.get shouldBe a[Subject]
      correctSubject.get.value shouldEqual(correctSubjectString)
    }

    "Return None for incorrect Subjects" in {
      Subject.createValidated("*NoT A \\ good subject") shouldBe None
    }

  }

}
