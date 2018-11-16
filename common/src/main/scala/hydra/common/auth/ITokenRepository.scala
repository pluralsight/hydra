package hydra.common.auth

import scala.concurrent.Future

trait ITokenRepository {
  def retrieveTokenInfo(token:String): Future[TokenInfo]
}