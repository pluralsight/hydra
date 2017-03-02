package hydra.core.http

import akka.http.scaladsl.server.directives.Credentials

/**
  * Created by alexsilva on 2/7/17.
  */
class SimpleAuthenticator extends Authenticator[String] {
  override def authenticate(credentials: Credentials) =
    credentials match {
      case p@Credentials.Provided(id) if p.verify("password") => Some(id)
      case _ => None
    }
}
