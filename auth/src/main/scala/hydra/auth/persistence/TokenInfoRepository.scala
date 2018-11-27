package hydra.auth.persistence

import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class TokenInfoRepository extends ITokenInfoRepository {
  import RepositoryModels._
  import TokenInfoRepository._

  val db = Database.forConfig("db")

  import slick.jdbc.PostgresProfile.api._

  def getByToken(token: String)
                (implicit ec: ExecutionContext): Future[TokenInfo] = {
    val query = (tokens join groups on (_.groupId === _.id)).map {
      case (t, g) =>
        (t.id, t.createdDate, t.modifiedDate, t.token, g.name) <>
          (TokenInfo.tupled, TokenInfo.unapply)
    }.result

    val x = db.run(query)
  }
}

object TokenInfoRepository {
  case class TokenInfo(token: String, resources: Set[String])
}


