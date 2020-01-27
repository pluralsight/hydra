package hydra.avro.resource

import hydra.avro.resource.HydraSubjectValidator.SubjectHasInvalidCharacters
import org.scalatest.{FlatSpec, Matchers}

class HydraSubjectValidatorSpec extends FlatSpec with Matchers with HydraSubjectValidator {

  it should "accept a valid subject containing character from each class" in {
    val subject = "exp.test.subject_123-2.456"
    assert(validateSubjectCharacters(subject).isValid)
  }

  it should "reject a subject containing special characters" in {
    val subject = "@#$%^&"
    validateSubjectCharacters(subject) match {
      case cats.data.Validated.Valid(_) => fail("Subject provided is invalid")
      case cats.data.Validated.Invalid(e) =>
        e.head.errorMessage shouldBe SubjectHasInvalidCharacters.errorMessage
    }
  }
}
