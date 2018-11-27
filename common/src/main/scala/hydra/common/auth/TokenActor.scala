package hydra.common.auth

import akka.actor.Actor
import akka.pattern.pipe

class TokenActor(val tokenInfoRepository: ITokenInfoRepository) extends Actor {

  private val cache = Map[String, TokenInfo]()
  private implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetToken(token) => {
      cache.get(token) match {
        case Some(tokenInfo) => sender ! tokenInfo
        case None => tokenInfoRepository.getByToken(token) pipeTo sender
      }
    }
  }
}

case class TokenInfo(token: String, resources: Set[String])

case class GetToken(token: String)
