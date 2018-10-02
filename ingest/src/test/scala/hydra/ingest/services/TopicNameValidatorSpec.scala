package hydra.ingest.services

import hydra.ingest.services.ErrorMessages._
import org.scalatest.{FlatSpec, Matchers}

class TopicNameValidatorSpec extends FlatSpec
  with Matchers {

  "A TopicNameValidator" should "return Valid for a valid topic" in {
    TopicNameValidator.validate("dvs.test.v1.TestEntity") shouldBe Valid
  }

  it should "return Invalid for a topic name longer than 249 characters" in {
    TopicNameValidator.validate("exp.test." + "a" * 250) shouldBe
      InvalidReport(Invalid(LengthError))
  }

  it should "return Invalid for a topic containing invalid characters" in {
    TopicNameValidator.validate("exp.test.Test(Topic)") shouldBe
      InvalidReport(Invalid(InvalidCharacterError))
  }

  it should "return Valid for a topic containing no invalid characters" in {
    TopicNameValidator.validate("exp.test-test.TestTopic") shouldBe
      Valid
  }

  it should "return invalid if doesn't start with a valid org prefix" in {
    TopicNameValidator.validate("false.test.TestTopic") shouldBe
      InvalidReport(Invalid(BadOrgError))
  }

  it should "be properly formatted by containing at least 3 segments" in {
    TopicNameValidator.validate("exp.TestTopic") shouldBe
      InvalidReport(Invalid(BadTopicFormatError))
  }

  it should "return multiple errors if validation fails for multiple reasons" in {
    TopicNameValidator.validate("falsetestTestTopic") shouldBe
      InvalidReport(Invalid(BadOrgError), Invalid(BadTopicFormatError))
  }
}
