package hydra.auth.actors

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.testkit.{TestKit, TestProbe}
import hydra.auth.actors.TokenActor.{GetTokenFromDB, RemoveTokenFromCache, RemoveTokenFromDB, TokenInvalidated}
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

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetTokenFromDB(tokenInfo.token), listener.ref)

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    listener.expectMsg(tokenInfo)
  }

  it should "return a failed future if a token can't be found in the cache or db" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(*, *)
      .returning(Future.failed(new RuntimeException()))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetTokenFromDB(tokenInfo.token), listener.ref)

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
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

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    tokenActor.tell(GetTokenFromDB(tokenInfo.token), listener.ref)

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    val info = listener.expectMsg(tokenInfo)

    tokenActor.tell(GetTokenFromDB(info.token), listener.ref)

  }

  it should "invalidate a token in the cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    tokenActor.tell(RemoveTokenFromCache(tokenInfo.token), listener.ref)

    listener.expectMsg(TokenInvalidated)

  }

  it should "remove a token from db and broadcast cluster message to remove cached tokens" in {
    val mediator = DistributedPubSub(system).mediator
    val probiño = TestProbe()
    mediator ! Subscribe(TokenActor.mediatorTag, probiño.ref)

    val tokenInfo = TokenGenerator.generateTokenInfo

    val repoStub = stub[ITokenInfoRepository]
    (repoStub.removeToken(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(true))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    tokenActor.tell(RemoveTokenFromDB(tokenInfo.token), probiño.ref)

    probiño.expectMsgAllOf(RemoveTokenFromCache(tokenInfo.token), true)

    (repoStub.removeToken(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once
  }

}