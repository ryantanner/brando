package brando

import scala.language.implicitConversions

class TypedRequest(val redisCommand: String, val params: () => Seq[String])

sealed trait Command

object Commands {

  def toRequest(r: TypedRequest): Request = {
    Request(r.redisCommand, r.params():_*)
  }

  case class Set(key: String)(value: String)(implicit extractor: Any => Option[String]) 
    extends TypedRequest(
      "SET", () => Seq(key, value)
    ) with Command

  case class Get(key: String)(implicit extractor: Any => Option[String])
    extends TypedRequest(
      "GET", () => Seq(key)
    ) with Command

  case object FlushDB extends TypedRequest(
      "FLUSHDB", () => Seq()
    ) with Command

}
