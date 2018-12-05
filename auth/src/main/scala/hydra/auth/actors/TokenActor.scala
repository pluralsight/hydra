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

import scala.concurrent.ExecutionContextExecutor

class TokenActor(val tokenInfoRepository: ITokenInfoRepository) extends Actor {

  import TokenActor._

  private val cache = GuavaCache[TokenInfo]
  private implicit val ec: ExecutionContextExecutor = context.dispatcher

  private val mediator = DistributedPubSub(context.system).mediator

  override def preStart(): Unit = {
    mediator ! Subscribe(mediatorTag, self)
  }

  override def receive: Receive = {
    case AddTokenToDB(token) =>
      val dbInsert = for {
        tokenString <- tokenInfoRepository.insertToken(token)
        tokenInfo <- tokenInfoRepository.getTokenInfo(token.token)
      } yield (tokenString, tokenInfo)

      dbInsert.map(_._1) pipeTo sender

      dbInsert.foreach {
        case (_, tokenInfo: TokenInfo) =>
          mediator ! Publish(mediatorTag, TokenActor.AddTokenInfoToCache(tokenInfo))
      }

    case AddTokenInfoToCache(tokenInfo) =>
      cache.put(tokenInfo.token)(tokenInfo)
      sender ! TokenCached(tokenInfo.token)

    case GetTokenFromDB(tokenString) =>
      cache.cachingF(tokenString)(None) {
        tokenInfoRepository.getTokenInfo(tokenString)
      } pipeTo sender

    case RemoveTokenInfoFromCache(tokenString) =>
      cache.remove(tokenString)
      sender ! TokenInvalidated(tokenString)

    case RemoveTokenFromDB(tokenString) =>
      val dbDelete = tokenInfoRepository.removeToken(tokenString)

      dbDelete pipeTo sender

      dbDelete.foreach { _ =>
        mediator ! Publish(mediatorTag, RemoveTokenInfoFromCache(tokenString))
      }
  }
}

object TokenActor {

  val mediatorTag = "token-actor"

  case class AddTokenToDB(token: Token)

  case class AddTokenInfoToCache(tokenInfo: TokenInfo)

  case class TokenCached(token: String)

  case class GetTokenFromDB(token: String)

  case class RemoveTokenInfoFromCache(token: String)

  case class RemoveTokenFromDB(token: String)

  case class TokenInvalidated(token: String)

  def props(tokenInfoRepo: ITokenInfoRepository): Props =
    Props(classOf[TokenActor], tokenInfoRepo)
}
