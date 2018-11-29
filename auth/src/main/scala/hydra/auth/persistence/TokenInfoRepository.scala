package hydra.auth.persistence

import hydra.core.persistence.PersistenceDelegate

import scala.concurrent.{ExecutionContext, Future}

class TokenInfoRepository(val persistenceDelegate: PersistenceDelegate) extends ITokenInfoRepository
  with RepositoryModels {

  import TokenInfoRepository._

  import persistenceDelegate.profile.api._

  val db = persistenceDelegate.db

  def getByToken(token: String)
                (implicit ec: ExecutionContext): Future[TokenInfo] = {

  }
}

object TokenInfoRepository {
  def apply(persistenceDelegate: PersistenceDelegate): TokenInfoRepository =
    new TokenInfoRepository(persistenceDelegate)

  case class TokenInfo(token: String, resources: Set[String])
}


