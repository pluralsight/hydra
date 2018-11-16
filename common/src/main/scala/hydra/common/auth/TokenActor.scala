package hydra.common.auth

import akka.actor.Actor
import akka.pattern.pipe

class TokenActor(val tokenRepository: ITokenRepository) extends Actor {

  private val cache = Map[String, TokenInfo]()
  private implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetToken(token) => {
      cache.get(token) match {
        case Some(tokenInfo) => sender ! tokenInfo
        case None => tokenRepository.retrieveTokenInfo(token) pipeTo sender
      }
    }
  }
}

case class TokenInfo(token:String)
case class GetToken(token:String)