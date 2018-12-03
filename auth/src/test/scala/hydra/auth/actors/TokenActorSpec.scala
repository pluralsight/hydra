package hydra.auth.actors

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import hydra.auth.actors.TokenActor.{GetToken, InvalidateToken, TokenInvalidated}
import hydra.auth.persistence.ITokenInfoRepository
import hydra.auth.util.TokenGenerator
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class TokenActorSpec extends TestKit(ActorSystem("token-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory {

  implicit val ec = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TokenActor" should "should get token from repo if not in cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(tokenInfo.token), listener.ref)

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    listener.expectMsg(tokenInfo)
  }
  "A TokenActor" should "return a failed future if a token can't be found in the cache or db" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .when(*, *)
      .returning(Future.failed(new RuntimeException()))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(tokenInfo.token), listener.ref)

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    listener.expectMsgPF() {
      case Failure(ex) => ex.isInstanceOf[RuntimeException]
    }
  }

  it should "return a token from the cache when it is present" in {

    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    tokenActor.tell(GetToken(tokenInfo.token), listener.ref)

    (repoStub.getByToken(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    val info = listener.expectMsg(tokenInfo)

    tokenActor.tell(GetToken(info.token), listener.ref)

  }

  it should "invalidate a token in the cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    tokenActor.tell(InvalidateToken(tokenInfo.token), listener.ref)

    listener.expectMsg(TokenInvalidated)

  }
}