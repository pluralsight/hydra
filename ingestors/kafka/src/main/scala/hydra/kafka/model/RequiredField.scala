package hydra.kafka.model

object RequiredField extends Enumeration {
  type RequiredField = String
  val DOC = "doc"
  val CREATED_AT = "createdAt"
  val UPDATED_AT = "updatedAt"
}
