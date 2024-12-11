package ml.oo4oo.utils

import java.util.Base64
import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import scala.reflect.ClassTag

trait Serialize {
  override def toString(): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(this)
    oos.close
    new String(Base64.getEncoder().encode(stream.toByteArray), UTF_8)
  }
}

object Serialize {
  def fromString[T: ClassTag](str: String): T = {
    val bytes = Base64.getDecoder().decode(str.getBytes(UTF_8))
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close

    val ct = implicitly[ClassTag[T]]
    value match {
      case ct(x) => x
      case _     => throw new Exception(s"object can't cast to type ${ct}")
    }
  }
}
