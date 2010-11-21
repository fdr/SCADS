package edu.berkeley.cs.scads.mapreduce

import scala.collection.mutable.{Buffer, ListBuffer, HashMap, Map}
import edu.berkeley.cs.avro.marker.AvroRecord

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.{Namespace, ScadsCluster}

trait Context {
  def reportStatus(msg: String): Unit
  def collect(key: AvroRecord, value: AvroRecord): Unit
}

abstract class ClientContext(cluster: ScadsCluster) {
  // map of namespace name to scads client.
  val clientCache = HashMap.empty[String, AnyRef ]

  def getKey[KeyType <: AvroRecord, ValueType <: AvroRecord]
      (nsName: String, key: KeyType)
      (implicit keyType: Manifest[KeyType],
       valueType: Manifest[ValueType]): Option[ValueType] = {
    val ns = clientCache.getOrElseUpdate(nsName,
        cluster.getNamespace[KeyType, ValueType]
        (nsName)).asInstanceOf[Namespace[KeyType, ValueType]]
    ns.get(key).asInstanceOf[Option[ValueType]]
  }
}

/**
 * A quick implementation of the MapperContext class.
 */
class MapperContext(cluster: ScadsCluster)
    extends ClientContext(cluster) with Context {
  
  val mapperOutput = HashMap.empty[ AvroRecord, Buffer[AvroRecord] ]
  
  def collect(key: AvroRecord, value: AvroRecord): Unit = {
    mapperOutput.getOrElseUpdate(key, ListBuffer[AvroRecord]()).append(value)
  }
  
  def reportStatus(msg: String): Unit = {
    println(msg)
  }
}

trait Mapper {
  def map(key: AvroRecord, value: AvroRecord, context: MapperContext): Unit
}

trait Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord], context: Context): Unit
}
