package hydra.kafka.model

import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike


class TopicMetadataV2TransportSpec extends AnyWordSpecLike with Matchers {

  "TopicMetadataV2Request" must {

    "Parse correct emails" in {
      val correctEmailAddress = "data-platform-team@pluralsight.com"
      val correctEmail = ContactMethod.Email.create(correctEmailAddress)
      correctEmail.get shouldBe a[ContactMethod.Email]
      correctEmail.get.address.value shouldEqual (correctEmailAddress)
    }

    "Return None for incorrect emails" in {
      ContactMethod.Email.create("data-platform-team@pluralsight") shouldBe None
    }

    "Parse correct slacks" in {
      val correctSlackChannel = "#dev-data-platform"
      val correctSlack = ContactMethod.Slack.create(correctSlackChannel)
      correctSlack.get shouldBe a[ContactMethod.Slack]
      correctSlack.get.channel.value shouldEqual (correctSlackChannel)
    }

    "Return None for incorrect slacks" in {
      ContactMethod.Slack.create("Not a slack address") shouldBe None
    }

    "Parse correct Subjects" in {
      val correctSubjectString = "skills.exp-blah-blah"
      val correctSubject = Subject.createValidated(correctSubjectString)
      correctSubject.get shouldBe a[Subject]
      correctSubject.get.value shouldEqual (correctSubjectString)
    }

    "Return None for incorrect Subjects" in {
      Subject.createValidated("*NoT A \\ good subject") shouldBe None
    }

    "Ensure correct topic prefixes are allowed" in {
      val correctPrefixList = List("skills.","flow.","tech.","fin.","dvs.")
      val listOfValidatedSubjects = correctPrefixList.map(pre => Subject.createValidated(pre + "blah.blah-blah-blah"))
      listOfValidatedSubjects.map(sub => sub.get shouldBe a[Subject])
    }

    "Ensure incorrect topic prefixes are not allowed" in {
      val correctPrefixList = List("ha.","bla.","exp.","hello.","world.")
      val listOfInvalidatedSubjects = correctPrefixList.map(pre => Subject.createValidated(pre + "blah.blah-blah-blah"))
      listOfInvalidatedSubjects.map(sub => sub.getOrElse(None) shouldBe None)
    }

    "Ensure underscores are not allowed" in {
      val correctPrefixList = List("skills.","flow.","tech.","fin.","dvs.")
      val listOfInvalidatedSubjects = correctPrefixList.map(pre => Subject.createValidated(pre + "blah.blah-blah_blah"))
      listOfInvalidatedSubjects.map(sub => sub.getOrElse(None) shouldBe None)
    }

    "return invalid subject if longer than 255 characters" in {
      val bigTopic = "dvs." + "a" * 252
      Subject.createValidated(bigTopic).getOrElse(None) shouldBe None
    }

    "return invalid subject containing .-" in {
      Subject.createValidated("dvs.-hello").getOrElse(None) shouldBe None
    }

    "return invalid subject containing -." in {
      Subject.createValidated("dvs.hello-.goodbye").getOrElse(None) shouldBe None
    }

    "Allow internal topics" in {
      Subject.createValidated("_hydra.v2.metadata").getOrElse(None) shouldBe a[Subject]
    }

  }

}
