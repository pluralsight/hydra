package hydra.auth.persistence

import hydra.auth.persistence.TokenInfoRepository.TokenInfo

import scala.concurrent.{ExecutionContext, Future}

trait ITokenInfoRepository {
  def getByToken(token:String)
                (implicit ec: ExecutionContext): Future[TokenInfo]
}
