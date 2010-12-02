package edu.berkeley.cs.scads.mapreduce

import scala.collection.mutable.{Buffer, ListBuffer, ArrayBuffer, HashMap, Map}
import edu.berkeley.cs.avro.marker.AvroRecord

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.{Namespace, ScadsCluster}

trait Context {
  val output = HashMap.empty[AvroRecord, Buffer[AvroRecord]]
  def reportStatus(msg: String): Unit
  def collect(key: AvroRecord, value: AvroRecord): Unit
}

abstract class ClientContext(cluster: ScadsCluster) {
  // map of namespace name to scads client.
  val clientCache = HashMap.empty[String, AnyRef]

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

class MapperContext(cluster: ScadsCluster)
    extends ClientContext(cluster) with Context {
    
  def collect(key: AvroRecord, value: AvroRecord): Unit = {
    output.getOrElseUpdate(key, ListBuffer[AvroRecord]()).append(value)
  }
  
  def reportStatus(msg: String): Unit = {
    println(msg)
  }
}

class ReducerContext(cluster: ScadsCluster)
    extends ClientContext(cluster) with Context {
  
  def collect(key: AvroRecord, value: AvroRecord): Unit = {
    // Reducers should only output 1 value per key.
    output.update(key, ArrayBuffer[AvroRecord](value))
  }
  
  def reportStatus(msg: String): Unit = {
    println(msg)
  }
}

trait Mapper {
  // data input is a sequence of key, value pairs.
  def map(data: Seq[(AvroRecord, AvroRecord)], context: MapperContext): Unit
}

trait Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord], context: ReducerContext): Unit
}
