package hydra.auth.persistence

import hydra.auth.persistence.RepositoryModels.{Resource, Token}
import hydra.auth.persistence.AuthRepository.TokenInfo

import scala.concurrent.{ExecutionContext, Future}

trait IAuthRepository {
  def getTokenInfo(token:String)
                  (implicit ec: ExecutionContext): Future[TokenInfo]

  def insertToken(token: Token)
                 (implicit ec: ExecutionContext): Future[Token]

  def removeToken(token: String)
                 (implicit ec: ExecutionContext): Future[String]

  def insertResource(resource: Resource)
                    (implicit ec: ExecutionContext): Future[Resource]
}
