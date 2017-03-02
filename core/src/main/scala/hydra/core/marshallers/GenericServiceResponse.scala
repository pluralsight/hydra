package hydra.core.marshallers

/**
  * Created by alexsilva on 2/21/17.
  */
trait ServiceResponse {
  def status: Int

  def message: String
}

case class GenericServiceResponse(status: Int, message: String) extends ServiceResponse

case class ExceptionalServiceResponse(status: Int, error: Throwable) extends ServiceResponse {
  override val message: String = error.getMessage
}


