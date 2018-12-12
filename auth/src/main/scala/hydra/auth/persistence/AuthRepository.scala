package hydra.auth.persistence

import hydra.auth.persistence.RepositoryModels.{Resource, Token}
import hydra.core.persistence.PersistenceDelegate

import scala.concurrent.{ExecutionContext, Future}

class AuthRepository(val persistenceDelegate: PersistenceDelegate) extends IAuthRepository
  with RepositoryModels {

  import AuthRepository._

  import persistenceDelegate.profile.api._

  val db = persistenceDelegate.db

  def getTokenInfo(token: String)
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
        case ((t, g), r) => (t.token, g.id, r.name)
      }
    }.filter(_._1 === token)

    db.run(query.result).map { resultTup =>
      if (resultTup.nonEmpty) {
        /*
        Since the token is not the primary key for the token table, it is in theory possible to have
         duplicate tokens which would result in multiple rows being returned for this query.  We
         are pulling off just the head result, which could lead to unexpected behavior.  Unlikely,
         but a potential gotcha if you are ever debugging this code wondering why your result looks
         weird.
         */
        TokenInfo(token, resultTup.map(_._2).head, resultTup.map(_._3).toSet)
      }
      else {
        throw new MissingTokenException(s"$token not found.")
      }
    }
  }

  def insertToken(token: Token)
                 (implicit ec: ExecutionContext): Future[Token] = {
    db.run(tokenTable += Token.unapply(token).get).map(_ => token)
  }

  def removeToken(token: String)
                 (implicit ec: ExecutionContext): Future[String] = {
    db.run(tokenTable.filter(_.token === token).delete).map(_ => token)
  }

  def insertResource(resource: Resource)(implicit ec: ExecutionContext): Future[Resource] = {
    db.run(resourceTable += Resource.unapply(resource).get).map(_ => resource)
  }
}

object AuthRepository {
  def apply(persistenceDelegate: PersistenceDelegate): AuthRepository =
    new AuthRepository(persistenceDelegate)

  case class TokenInfo(token: String, groupId: Int, resources: Set[String])

  class MissingTokenException(msg: String) extends RuntimeException(msg)

}


