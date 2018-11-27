package hydra.common.auth

import org.joda.time.DateTime
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class TokenInfoRepository extends ITokenInfoRepository {
  import RepositoryComponents._

  val db = Database.forConfig("db")

  import slick.jdbc.PostgresProfile.api._

  def getByToken(token: String)
                (implicit ec: ExecutionContext): Future[TokenInfo] = {
    val query = (tokens join groups on (_.groupId === _.id)).map {
      case (t, g) =>
        (t.id, t.createdDate, t.modifiedDate, t.token, g.name) <>
          (TokenInfo.tupled, TokenInfo.unapply _)
    }.result

    val x = db.run(query)
  }
}

case class Token(id: Int, createdDate: DateTime, modifiedDate: DateTime, token: String,
                 groupId: String)

case class Group(id: Int, name: String, createdDate: DateTime, modifiedDate: DateTime)

case class Resource(id: Int, name: String, groupId: Int)
