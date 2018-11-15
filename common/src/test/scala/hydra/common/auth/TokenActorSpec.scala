package hydra.common.auth

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.pipe
import akka.testkit.{TestKit, TestProbe}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future

class TokenActorSpec extends TestKit(ActorSystem("token-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TokenActor" should "should get token from repo if not in cache" in {
    val token = "token"

    val listener = TestProbe()

    val repoStub = stub[ITokenRepository]

    (repoStub.retrieveTokenInfo _)
      .when(token)
      .returning(Future.successful(TokenInfo(token)))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(token), listener.ref)

    // assert repoStub was called

    (repoStub.retrieveTokenInfo _)
      .verify(token)
      .once

    listener.expectMsg(TokenInfo(token))
  }
  "A TokenActor" should "return a failed future if a token can't be found in the cache or db" in {
    val token = "token"

    val listener = TestProbe()

    val repoStub = stub[ITokenRepository]

    (repoStub.retrieveTokenInfo _)
      .when(token)
      .returning(Future.failed(new RuntimeException()))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(token), listener.ref)

    // assert repoStub was called

    (repoStub.retrieveTokenInfo _)
      .verify(token)
      .once

    listener.expectMsgPF() {
      case Failure(ex) => ex.isInstanceOf[RuntimeException]
    }
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