package edu.berkeley.cs.scads.comm

import java.io._
import java.nio.ByteBuffer
import edu.berkeley.cs.avro.marker.AvroRecord


object SerializeUtil {
  def fromByteArray[ResultType](bytes: Array[Byte]): ResultType = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    return ois.readObject.asInstanceOf[ResultType]
  }

  def toByteArray(obj: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    return baos.toByteArray()
  }
}


/**
 * Remote Closure for a class. This is a fake remote closure since true remote
 * closure would require us to serialize the bytecode of the class, not just
 * the signature of the class.
 */
case class RemoteClassClosure(var rawbytes: Array[Byte]) extends AvroRecord {
  def retrieveClass(): Class[_] = {
    SerializeUtil.fromByteArray[ Class[_] ](rawbytes)
  }
}


object RemoteClassClosure {
  def create(c: Class[_]): RemoteClassClosure = {
    RemoteClassClosure(SerializeUtil.toByteArray(c))
  }
}

