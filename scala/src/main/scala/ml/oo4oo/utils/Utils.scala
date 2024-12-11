package ml.oo4oo.utils

import ml.combust.mleap.core.types.{StructField, StructType}
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._

import java.util.{Base64, UUID}
import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable.Stack
import scala.reflect.ClassTag
import scala.util.Try

object Utils {
  implicit val formats = org.json4s.DefaultFormats

  def toJson(map: Map[String, Any]): String = Serialization.write(map)

  def fromJson(json: String): Map[String, Any] = parse(json).values.asInstanceOf[Map[String, Any]]

  def structTypeEncode(schema: StructType): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(schema.fields)
    oos.close
    new String(Base64.getEncoder().encode(stream.toByteArray), UTF_8)
  }

  def structTypeDecode(str: String): StructType = {
    val bytes = Base64.getDecoder().decode(str.getBytes(UTF_8))
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject.asInstanceOf[Seq[StructField]]
    ois.close
    StructType(value).get
  }

  /*import org.apache.commons.codec.binary.Base64;

  def UUID(): String = {
    val uid = uuid.randomUUID().toString
    val b   = new String(Base64.encodeBase64String(uid.getBytes))
    b.substring(0, 16).toLowerCase()
  }*/

  def uuid[T: ClassTag](prefix: String, value: T) = {
    val array: Array[String] = value match {
      case x: String        => Array(x)
      case x: Array[String] => x
      case x: Seq[String]   => x.toArray
      case _                => Array.empty[String]
    }

    val simpleName = array
      .map(x => {
        val arr = x.split("_")
        if (arr.size == 3 && arr(0).length == 3 && arr(1).length == 12) arr(2) else x
      })
      .mkString("+")

    prefix + "_" + UUID.randomUUID().toString.takeRight(12) + "_" + simpleName
  }

  def formatToTree(lines: Seq[String]): Seq[String] = {
    var stack = Stack[(Int, Int)]()
    val tabs  = lines.map { line => line.prefixLength(_ == '\t') }

    lines.zipWithIndex.map { case (line, idx) =>
      val level = line.prefixLength(_ == '\t')

      val v                = if (stack.size > 0) stack.top._1 else Int.MaxValue
      val nextSameLevelIdx = Try {
        tabs.map(_ == level).zipWithIndex.filter(_._1).map(_._2).filter(_ > idx).filter(_ < v).max
      }.getOrElse(-1)
      if (nextSameLevelIdx >= 0 && (stack.size == 0 || nextSameLevelIdx < stack.top._1)) {
        stack.push((nextSameLevelIdx, level))
      }

      var prefix = "   " * level + "+- "
      stack.map { x =>
        if (prefix.size > x._2 * 3 && x._2 * 3 >= 0 && idx != x._1) {
          prefix = prefix.updated(x._2 * 3, ':')
        }
      }

      if (stack.size > 0 && idx == stack.top._1) {
        stack.pop
      }

      prefix + line.trim
    }
  }
}
