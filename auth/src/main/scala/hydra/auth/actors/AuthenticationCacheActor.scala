package hydra.auth.actors

import akka.actor.{Actor, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.pipe
import hydra.auth.persistence.AuthRepository.TokenInfo
import hydra.auth.persistence.IAuthRepository
import hydra.auth.persistence.RepositoryModels.{Resource, Token}
import scalacache.guava._
import scalacache.modes.scalaFuture._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class AuthenticationCacheActor(val authRepository: IAuthRepository) extends Actor {

  import AuthenticationCacheActor._

  private val cache = GuavaCache[TokenInfo]
  private implicit val ec: ExecutionContextExecutor = context.dispatcher

  private val mediator = DistributedPubSub(context.system).mediator

  override def preStart(): Unit = {
    mediator ! Subscribe(MediatorTag, self)
  }

  override def receive: Receive =
    insertOps orElse
      deleteOps orElse {
      case GetTokenInfo(tokenString) =>
        cache.cachingF(tokenString)(Some(1.second)) {
          authRepository.getTokenInfo(tokenString)
        } pipeTo sender
    }

  def insertOps: Receive = {
    case AddTokenToDB(token) =>
      val dbInsert = for {
        tokenString <- authRepository.insertToken(token)
        tokenInfo <- authRepository.getTokenInfo(token.token)
      } yield (tokenString, tokenInfo)

      dbInsert.map(_._1) pipeTo sender

      dbInsert.foreach {
        case (_, tokenInfo: TokenInfo) =>
          mediator ! Publish(MediatorTag, AuthenticationCacheActor.AddTokenToCache(tokenInfo))
      }

    case AddTokenToCache(tokenInfo) =>
      cache.put(tokenInfo.token)(tokenInfo)
      sender ! TokenCached(tokenInfo.token)

    case AddResourceToDB(tokenString, resource) =>
      val dbFuture = for {
        rsc <- authRepository.insertResource(resource)
        tokenInfo <- authRepository.getTokenInfo(tokenString)
      } yield (rsc, tokenInfo)

      dbFuture.map(_._1) pipeTo sender()
      dbFuture.foreach { case (_, tokenInfo: TokenInfo) =>
        mediator ! Publish(MediatorTag, AddTokenToCache(tokenInfo))
      }
  }

  def deleteOps: Receive = {
    case RemoveTokenFromCache(tokenString) =>
      cache.remove(tokenString)
      sender ! TokenInvalidated(tokenString)

    case RemoveTokenFromDB(tokenString) =>
      val dbDelete = authRepository.removeToken(tokenString)

      dbDelete pipeTo sender

      dbDelete.foreach { _ =>
        mediator ! Publish(MediatorTag, RemoveTokenFromCache(tokenString))
      }
  }
}

object AuthenticationCacheActor {

  val MediatorTag = "token-actor"

  case class AddTokenToDB(token: Token)

  case class AddResourceToDB(token: String, resource: Resource)

  case class AddTokenToCache(tokenInfo: TokenInfo)

  case class TokenCached(token: String)

  case class GetTokenInfo(token: String)

  case class RemoveTokenFromCache(token: String)

  case class RemoveTokenFromDB(token: String)

  case class TokenInvalidated(token: String)

  def props(tokenInfoRepo: IAuthRepository): Props =
    Props(classOf[AuthenticationCacheActor], tokenInfoRepo)
}
