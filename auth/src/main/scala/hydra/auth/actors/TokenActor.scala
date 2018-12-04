package hydra.auth.actors

import akka.actor.{Actor, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import hydra.auth.persistence.ITokenInfoRepository
import hydra.auth.persistence.TokenInfoRepository.TokenInfo
import akka.pattern.pipe
import hydra.auth.persistence.RepositoryModels.Token
import scalacache.modes.scalaFuture._
import scalacache.guava._

class TokenActor(val tokenInfoRepository: ITokenInfoRepository) extends Actor {

  import TokenActor._
  // Todo distributed pub sub for broadcasting

  private val cache = GuavaCache[TokenInfo]
  private implicit val ec = context.dispatcher

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(mediatorTag, self)

  override def receive: Receive = {
    case AddTokenToDB(token) =>
      tokenInfoRepository.insertToken(token).flatMap{ _ =>
        tokenInfoRepository.getTokenInfo(token.token).map { tokenInfo =>
          mediator ! Publish(mediatorTag, TokenActor.AddTokenToCache(tokenInfo))

        }
      } pipeTo sender
    case AddTokenToCache(tokenInfo) =>
      cache.put(tokenInfo.token)(tokenInfo)
    case GetTokenFromDB(token) =>
      cache.cachingF(token)(None) {
        tokenInfoRepository.getTokenInfo(token)
      } pipeTo sender

    case RemoveTokenFromCache(token) =>
      cache.remove(token)
      sender ! TokenInvalidated

    case RemoveTokenFromDB(token) =>
      tokenInfoRepository.removeToken(token).map{ result =>
        mediator ! Publish(mediatorTag, RemoveTokenFromCache(token))
        result
      } pipeTo sender

  }
}

object TokenActor {

  val mediatorTag = "token-actor"

  case class AddTokenToDB(token: Token)

  case class AddTokenToCache(tokenInfo: TokenInfo)

  case class GetTokenFromDB(token: String)

  case class RemoveTokenFromCache(token: String)

  case class RemoveTokenFromDB(token: String)

  case object TokenInvalidated

  def props(tokenInfoRepo: ITokenInfoRepository): Props =
    Props(classOf[TokenActor], tokenInfoRepo)
}
