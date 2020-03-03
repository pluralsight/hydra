package hydra.core

@SerialVersionUID(1L)
class HydraException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)
    with Serializable {
  def this(msg: String) = this(msg, null)
}
