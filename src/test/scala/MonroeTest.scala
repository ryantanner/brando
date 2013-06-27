package brando

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._

import akka.actor._
import akka.actor.Status._
import scala.concurrent.duration._
import akka.util.ByteString
import akka.event._

import akka.channels._

import scala.language.implicitConversions

class MonroeTest extends TestKit(ActorSystem("MonroeTest")) with FunSpec {

  implicit def asString(value: Any) = Response.AsString.unapply(value)

  def testSender = {
    val sender = TestProbe()
    val log = Logging(system, sender.ref)

    sender.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any) = {
        log.debug("msg: " + msg.toString)
        this
      }
    })

    val senderChannel = new ChannelRef[(Option[StatusReply], TypedRequest) :+: TNil](sender.ref)
    (sender, senderChannel, log)
  }

  describe("set") {
    it("should respond with OK") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Set("mykey")("somevalue")

      sender.expectMsg(Some(Ok))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }
  }

  describe("get") {
    it("should respond with value option for existing key") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Set("mykey")("somevalue")

      sender.expectMsg(Some(Ok))

      monroe <-!- Commands.Get("mykey")

      sender.expectMsg(Some(ByteString("somevalue")))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }

    it("should respond with None for non-existent key") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Get("mykey")

      sender.expectMsg(None)
    }
  }

  describe("incr") {
    it("should increment and return value for existing key") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Set("incr-test")("10")

      sender.expectMsg(Some(Ok))

      monroe <-!- Commands.Incr("incr-test")

      sender.expectMsg(Some(11))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }

    it("should return 1 for non-existent key") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Incr("incr-test")

      sender.expectMsg(Some(1))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }
  }

  describe("sadd") {
    it("should return number of members added to set") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Sadd("sadd-test")("one")

      sender.expectMsg(Some(1))

      monroe <-!- Commands.Sadd("sadd-test")("two", "three")

      sender.expectMsg(Some(2))

      monroe <-!- Commands.Sadd("sadd-test")("one", "four")

      sender.expectMsg(Some(1))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }
  }

  describe("smembers") {
    it("should return all members in a set") {
      implicit val (sender, senderChannel, _) = testSender
      val monroe = Monroe(system)

      monroe <-!- Commands.Sadd("smembers-test")("one", "two", "three", "four")

      sender.expectMsg(Some(4))

      monroe <-!- Commands.Smembers("smembers-test")

      val resp = sender.receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
      assert(resp.getOrElse(List()).toSet ===
        Set(Some(ByteString("one")), Some(ByteString("two")),
          Some(ByteString("three")), Some(ByteString("four"))))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }

  }

  describe("expiry") {
    it("should mixin expiry on SET") {
      implicit val (sender, senderChannel, log) = testSender
      val monroe = Monroe(system)

      monroe <-!- (new Commands.Set("mykey")("myvalue") withExpiry 10)

      log.debug("expiry " + (new Commands.Set("mykey")("myvalue")
      withExpiry 10).toRequest.toString)

      sender.expectMsg(Some(Ok))

      monroe <-!- Commands.TTL("mykey")
      sender.expectMsg(Some(10))

      monroe <-!- Commands.FlushDB
      sender.expectMsg(Some(Ok))
    }
  }

  describe("piplining") {
    it("should respond to a Seq of multiple requests all at once") {
      implicit val (sender, senderChannel, log) = testSender
      val monroe = Monroe(system)
      val ping = Commands.Ping

      monroe <-!- ping
      monroe <-!- ping
      monroe <-!- ping

      sender.expectMsg(Some(Pong))
      sender.expectMsg(Some(Pong))
      sender.expectMsg(Some(Pong))

    }
  }
  /*

    it("should support pipelines of setex commands") {
      val brando = system.actorOf(Brando())
      val setex = Request("SETEX", "pipeline-setex-path", "10", "Some data")

      brando ! setex
      brando ! setex
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
    }

    it("should receive responses in the right order") {
      val brando = system.actorOf(Brando())
      val ping = Request("PING")
      val setex = Request("SETEX", "pipeline-setex-path", "10", "Some data")

      brando ! setex
      brando ! ping
      brando ! setex
      brando ! ping
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
    }
  }

  describe("large data sets") {
    it("should read and write large files") {
      import java.io.{ File, FileInputStream }

      val file = new File("src/test/resources/crime_and_punishment.txt")
      val in = new FileInputStream(file)
      val bytes = new Array[Byte](file.length.toInt)
      in.read(bytes)
      in.close()

      val largeText = new String(bytes, "UTF-8")

      val brando = system.actorOf(Brando())

      brando ! Request("SET", "crime+and+punishment", largeText)

      expectMsg(Some(Ok))

      brando ! Request("GET", "crime+and+punishment")

      expectMsg(Some(ByteString(largeText)))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("error reply") {
    it("should receive a failure with the redis error message") {
      val brando = system.actorOf(Brando())

      brando ! Request("SET", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR wrong number of arguments for 'set' command")
      }

      brando ! Request("EXPIRE", "1", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR value is not an integer or out of range")
      }
    }
  }

  describe("select") {
    it("should execute commands on the selected database") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Request("SET", "mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Request("GET", "mykey")

      expectMsg(Some(ByteString("somevalue")))

      brando ! Request("SELECT", "0")

      expectMsg(Some(Ok))

      brando ! Request("GET", "mykey")

      expectMsg(None)

      brando ! Request("SELECT", "5")
      expectMsg(Some(Ok))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("multi") {
    it("should return the multi responses after the exec command") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Request("MULTI")
      expectMsg(Some(Ok))

      brando ! Request("SET", "mykey", "somevalue")
      expectMsg(Some(Queued))

      brando ! Request("GET", "mykey")
      expectMsg(Some(Queued))

      brando ! Request("EXEC")
      expectMsg(Some(List(Some(Ok), Some(ByteString("somevalue")))))
    }

    it("should pipeline multi requests") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Request("MULTI")
      brando ! Request("SET", "mykey", "somevalue")
      brando ! Request("GET", "mykey")
      brando ! Request("EXEC")

      expectMsg(Some(Ok))
      expectMsg(Some(Queued))
      expectMsg(Some(Queued))
      expectMsg(Some(List(Some(Ok), Some(ByteString("somevalue")))))
    }

  }
  */
}

class LoggingActor(underlying: ActorRef) extends Actor {

  def receive = {
    LoggingReceive {
      case x ⇒ underlying.tell(x, sender)
    }
  }
}
