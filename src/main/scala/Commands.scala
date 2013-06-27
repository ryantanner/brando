package brando

import scala.language.implicitConversions

class TypedRequest(val redisCommand: String, val params: Seq[String]) {

  def toRequest: Request = Request(redisCommand, params:_*)

}

sealed trait Command { self: TypedRequest =>

  def withExpiry(expireIn: Int): TypedRequest = {
    new TypedRequest(self.redisCommand, (self.params ++ Seq("EX", expireIn.toString)))
  }

}

object Commands {

  case class Set(key: String)(value: String)(implicit extractor: Any => Option[String]) 
    extends TypedRequest(
      "SET", Seq(key, value)
    ) with Command

  case class Get(key: String)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "GET", Seq(key)
    ) with Command

  case class Incr(key: String)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "INCR", Seq(key)
    ) with Command

  case class Sadd(key: String)(elems: String*)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "SADD", Seq(key) ++ elems
    ) with Command

  case class Smembers(key: String)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "SMEMBERS", Seq(key)
    ) with Command

  case class TTL(key: String)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "TTL", Seq(key)
    ) with Command

  case object FlushDB extends TypedRequest(
      "FLUSHDB", Nil
    ) with Command

  case object Ping extends TypedRequest(
      "PING", Nil
    ) with Command

  case object Pong extends TypedRequest(
      "PONG", Nil
    ) with Command

}
