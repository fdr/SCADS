package edu.berkeley.cs.scads.comm

import java.io._
import java.nio.ByteBuffer
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.marker.AvroUnion

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

@serializable
class RemoteClosure(keyType: Class[_], valueType: Class[_],
                    mapFn: Function2[_, _, _],
                    aggFn: Option[Function2[_, _, _]]) {
  val keyTypeClass = keyType
  val valueTypeClass = valueType
  val mapFnBytes = Closure(mapFn)
  val aggFnBytes = aggFn match {
    case None => None
    case Some(fn) => Some(Closure(fn))
  }
}
