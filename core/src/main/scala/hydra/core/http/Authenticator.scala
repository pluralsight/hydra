package hydra.core.http

import akka.http.scaladsl.server.directives.Credentials

/**
  * Created by alexsilva on 2/7/17.
  */
trait Authenticator[T] {

  def authenticate(credentials: Credentials): Option[T]
}
