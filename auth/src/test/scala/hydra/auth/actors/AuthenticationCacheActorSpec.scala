package hydra.auth.actors

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.testkit.{TestKit, TestProbe}
import hydra.auth.actors.AuthenticationCacheActor._
import hydra.auth.persistence.IAuthRepository
import hydra.auth.persistence.RepositoryModels.{Resource, Token}
import hydra.auth.util.TokenGenerator
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class AuthenticationCacheActorSpec extends TestKit(ActorSystem("token-actor-spec"))
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

    val repoStub = stub[IAuthRepository]

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetTokenInfo(tokenInfo.token), listener.ref)

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    listener.expectMsg(tokenInfo)
  }

  it should "return a failed future if a token can't be found in the cache or db" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[IAuthRepository]

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(*, *)
      .returning(Future.failed(new RuntimeException()))

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetTokenInfo(tokenInfo.token), listener.ref)

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

    val repoStub = stub[IAuthRepository]

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo))

    tokenActor.tell(GetTokenInfo(tokenInfo.token), listener.ref)

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once

    val info = listener.expectMsg(tokenInfo)

    tokenActor.tell(GetTokenInfo(info.token), listener.ref)

  }

  it should "add a token to the cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[IAuthRepository]

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    tokenActor.tell(AddTokenInfoToCache(tokenInfo), listener.ref)

    listener.expectMsg(TokenCached(tokenInfo.token))
  }

  it should "invalidate a token in the cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[IAuthRepository]

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    tokenActor.tell(RemoveTokenFromCache(tokenInfo.token), listener.ref)

    listener.expectMsg(TokenInvalidated(tokenInfo.token))

  }

  it should "add a new token to the db and broadcast cluster message to add token to cache" in {
    val token = Token(
      111,
      DateTime.parse("2000-03-15"),
      DateTime.parse("2000-03-15"),
      "insert-token",
      1
    )

    val tokenInfo = TokenGenerator.generateTokenInfo

    val mediator = DistributedPubSub(system).mediator

    val listener = TestProbe()

    mediator ! Subscribe(AuthenticationCacheActor.MediatorTag, listener.ref)

    val repoStub = stub[IAuthRepository]

    (repoStub.insertToken(_: Token)(_: ExecutionContext))
      .when(token, *)
      .returning(Future.successful(token))

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(token.token, *)
      .returning(Future.successful(tokenInfo))

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    tokenActor.tell(AddTokenToDB(token), listener.ref)

    listener.expectMsgAllOf(AddTokenInfoToCache(tokenInfo), token)

    (repoStub.insertToken(_: Token)(_: ExecutionContext))
      .verify(token, *)
      .once
  }

  it should "remove a token from db and broadcast cluster message to remove cached tokens" in {
    val mediator = DistributedPubSub(system).mediator

    val probiño = TestProbe()

    mediator ! Subscribe(AuthenticationCacheActor.MediatorTag, probiño.ref)

    val tokenInfo = TokenGenerator.generateTokenInfo

    val repoStub = stub[IAuthRepository]

    (repoStub.removeToken(_: String)(_: ExecutionContext))
      .when(tokenInfo.token, *)
      .returning(Future.successful(tokenInfo.token))

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    tokenActor.tell(RemoveTokenFromDB(tokenInfo.token), probiño.ref)

    probiño.expectMsgAllOf(RemoveTokenFromCache(tokenInfo.token), tokenInfo.token)

    (repoStub.removeToken(_: String)(_: ExecutionContext))
      .verify(tokenInfo.token, *)
      .once
  }

  it should "add a resource to the db and broadcast a message to the cluster to update caches" in {
    val mediator = DistributedPubSub(system).mediator

    val probiña = TestProbe()

    mediator ! Subscribe(AuthenticationCacheActor.MediatorTag, probiña.ref)

    val tokenInfo = TokenGenerator.generateTokenInfo

    val resource = Resource(0, "test-resource", "topic", 1)

    val repoStub = stub[IAuthRepository]

    (repoStub.insertResource(_: Resource)(_: ExecutionContext))
      .when(resource, *)
      .returning(Future.successful(resource))

    val expectedTokenInfo = tokenInfo.copy(resources = tokenInfo.resources ++ Set(resource.name))

    (repoStub.getTokenInfo(_: String)(_: ExecutionContext))
      .when(*, *)
      .returning(Future.successful(expectedTokenInfo))

    val tokenActor = system.actorOf(Props(classOf[AuthenticationCacheActor], repoStub))

    tokenActor.tell(
      AddResourceToDB(tokenInfo.token, resource),
      probiña.ref
    )

    probiña.expectMsgAllOf(AddTokenInfoToCache(expectedTokenInfo), resource)

    (repoStub.insertResource(_: Resource)(_: ExecutionContext))
      .verify(resource, *)
      .once
  }
}
