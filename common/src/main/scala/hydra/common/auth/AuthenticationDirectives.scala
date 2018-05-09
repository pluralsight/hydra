package hydra.common.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.server.directives.{AuthenticationDirective, SecurityDirectives}
import hydra.common.Settings

import scala.concurrent.ExecutionContext

trait AuthenticationDirectives extends SecurityDirectives {

  private val authenticator = Settings.HydraSettings.Authenticator

  protected[this] implicit def ec: ExecutionContext

  protected[this] implicit def system: ActorSystem

  def authenticate: AuthenticationDirective[String] =
    authenticateOrRejectWithChallenge(authenticator.authenticate _)

  def authenticateWith(authenticator: HydraAuthenticator): AuthenticationDirective[String] =
    authenticateOrRejectWithChallenge(authenticator.authenticate _)
}
