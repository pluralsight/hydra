package hydra.auth.actors

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import hydra.auth.persistence.ITokenInfoRepository
import hydra.auth.persistence.TokenInfoRepository.TokenInfo

class TokenActor(val tokenInfoRepository: ITokenInfoRepository) extends Actor {
  import TokenActor._

  // TODO add scalacache
  private val cache = Map[String, TokenInfo]()
  private implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetToken(token) =>
      cache.get(token) match {
        case Some(tokenInfo) => sender ! tokenInfo
        case None => tokenInfoRepository.getByToken(token) pipeTo sender
      }
  }
}

object TokenActor {
  case class GetToken(token: String)
  def props(): Props =
    Props(classOf[TokenActor])
}
