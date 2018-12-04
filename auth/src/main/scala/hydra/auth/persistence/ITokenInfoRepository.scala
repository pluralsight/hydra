package hydra.auth.persistence

import hydra.auth.persistence.RepositoryModels.Token
import hydra.auth.persistence.TokenInfoRepository.TokenInfo

import scala.concurrent.{ExecutionContext, Future}

trait ITokenInfoRepository {
  def getTokenInfo(token:String)
                  (implicit ec: ExecutionContext): Future[TokenInfo]

  def insertToken(token: Token)
                 (implicit ec: ExecutionContext): Future[Boolean]

  def removeToken(token: String)
                 (implicit ec: ExecutionContext): Future[Boolean]
}
