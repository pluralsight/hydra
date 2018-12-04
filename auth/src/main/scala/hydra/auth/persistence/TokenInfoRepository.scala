package hydra.auth.persistence

import hydra.auth.persistence.RepositoryModels.Token
import hydra.core.persistence.PersistenceDelegate

import scala.concurrent.{ExecutionContext, Future}

class TokenInfoRepository(val persistenceDelegate: PersistenceDelegate) extends ITokenInfoRepository
  with RepositoryModels {

  import TokenInfoRepository._

  import persistenceDelegate.profile.api._

  val db = persistenceDelegate.db

  def getByToken(token: String)
                (implicit ec: ExecutionContext): Future[TokenInfo] = {
    /*
     * The group table has a 1:M relationship with both token and resource, so use it as a bridge to
     * return the token and its associated resources.
     */
    val query = {
      tokenTable join groupTable on {
        case (t, g) => t.groupId === g.id
      } join resourceTable on {
        case ((_, g), r) => g.id === r.groupId
      } map {
        case ((t, g), r) => (t.token, r.name)
      }
    }.filter(_._1 === token)

    db.run(query.result).map { resultTup =>
      if (resultTup.nonEmpty) {
        TokenInfo(token, resultTup.map(_._2).toSet)
      }
      else {
        throw new MissingTokenException(s"$token not found.")
      }
    }
  }

  def insertToken(token: Token)
                 (implicit ec: ExecutionContext): Future[Boolean] = {
    runAction(tokenTable += Token.unapply(token).get)
  }

  def removeToken(token: String)
                 (implicit ec: ExecutionContext): Future[Boolean] = {
    runAction(tokenTable.filter(_.token === token).delete)
  }

  private[persistence] def runAction[R](action: DBIOAction[R, NoStream, Nothing])
                                       (implicit ec: ExecutionContext): Future[Boolean] = {
    db.run(action).map {
      case 0 => false
      case _ => true
    }
  }
}

object TokenInfoRepository {
  def apply(persistenceDelegate: PersistenceDelegate): TokenInfoRepository =
    new TokenInfoRepository(persistenceDelegate)

  case class TokenInfo(token: String, resources: Set[String])

  class MissingTokenException(msg: String) extends RuntimeException(msg)

}


