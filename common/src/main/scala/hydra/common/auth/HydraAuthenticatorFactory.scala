package hydra.common.auth

@throws(classOf[ClassNotFoundException])
class HydraAuthenticatorFactory(authEnabled: Boolean, authenticatorClassName: String) {
  if (authEnabled) {
    val Authenticator: HydraAuthenticator =
      Class.forName(authenticatorClassName).newInstance().asInstanceOf[HydraAuthenticator]
    Authenticator
  }
}

object HydraAuthenticatorFactory {
  def apply(authEnabled: Boolean, authenticatorClassName: Option[String]): Option[HydraAuthenticator] = {
    if (authEnabled) {
      authenticatorClassName match {
        case Some(className) => getAuthenticator(className)
        case None => throw new ClassNotFoundException
      }
    }
    else {
      None
    }
  }

  private def getAuthenticator(authenticatorClassName: String) = {
    val Authenticator: HydraAuthenticator =
      Class.forName(authenticatorClassName).newInstance().asInstanceOf[HydraAuthenticator]
    Some(Authenticator)
  }
}
