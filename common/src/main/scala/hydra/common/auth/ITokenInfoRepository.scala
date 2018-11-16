package hydra.common.auth

import scala.concurrent.{ExecutionContext, Future}

trait ITokenInfoRepository {
  def getByToken(token:String)(implicit ec: ExecutionContext): Future[TokenInfo]
}