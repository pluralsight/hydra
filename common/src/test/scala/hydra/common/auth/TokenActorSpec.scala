package hydra.common.auth

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future
import akka.pattern.pipe

class TokenActorSpec extends TestKit(ActorSystem("token-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory
  with ImplicitSender {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TokenActor" should "should get token from repo if not in cache" in {
    val token = "token"

    val repoStub = stub[ITokenRepository]

    (repoStub.retrieveTokenInfo _)
      .when(token)
      .returning(Future.successful(TokenInfo(token)))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor ! GetToken(token)

    // assert repoStub was called

    (repoStub.retrieveTokenInfo _)
      .verify(token)
      .once

    expectMsg(TokenInfo(token))
  }
}

class TokenActor(val tokenRepository: ITokenRepository) extends Actor {

  val cache = Map[String, TokenInfo]()
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

trait ITokenRepository {
  def retrieveTokenInfo(token:String): Future[TokenInfo]
}

case class TokenInfo(token:String)
case class GetToken(token:String)