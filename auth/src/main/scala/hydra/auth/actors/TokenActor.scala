package hydra.auth.actors

import akka.actor.{Actor, Props}
import hydra.auth.persistence.ITokenInfoRepository
import hydra.auth.persistence.TokenInfoRepository.TokenInfo
import akka.pattern.pipe

class TokenActor(val tokenInfoRepository: ITokenInfoRepository) extends Actor {
  import TokenActor._

  // TODO add scalacache
  private val cache = scala.collection.mutable.Map[String, TokenInfo]()
  private implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetToken(token) =>
      cache.get(token) match {
        case Some(tokenInfo) => sender ! tokenInfo
        case None => {
          val s = sender
          tokenInfoRepository.getByToken(token).map {
            tokenInfo =>
              cache += (tokenInfo.token -> tokenInfo)
              tokenInfo
          } pipeTo s
        }
      }

    case InvalidateToken(token) =>
      cache.remove(token)
      sender ! TokenInvalidated
  }
}

object TokenActor {
  case class GetToken(token: String)
  case class InvalidateToken(token: String)
  case object TokenInvalidated
  def props(tokenInfoRepo: ITokenInfoRepository): Props =
    Props(classOf[TokenActor], tokenInfoRepo)
}
