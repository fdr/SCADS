package edu.berkeley.cs.scads.mapreduce

import scala.collection.JavaConversions._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import routing._
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.util.Utf8
import net.lag.logging.Logger
import org.apache.zookeeper.CreateMode
import java.nio.ByteBuffer
import actors.Actor
import java.util.concurrent.TimeUnit
import collection.mutable.{ArrayBuffer, MutableList, HashMap}
import java.util.Arrays
import scala.concurrent.ManagedBlocker

import edu.berkeley.cs.scads.comm.RemoteClassClosure

trait MapReduceExecutor[KeyType <: IndexedRecord, 
                       ValueType <: IndexedRecord, 
                       RecordType <: IndexedRecord,
                       RangeType] 
  extends AvroComparator {
  this: Namespace[KeyType, ValueType, RecordType, RangeType] 
        with QuorumProtocol[KeyType, ValueType, RecordType, RangeType]
        with RoutingProtocol[KeyType, ValueType, RecordType, RangeType]
        with SimpleMetaData[KeyType, ValueType, RecordType, RangeType] 
        with AvroSerializing[KeyType, ValueType, RecordType, RangeType] =>
 
  implicit val keyType: Manifest[KeyType]
  implicit val valueType: Manifest[ValueType]

  /**
   * Execute mappers on the specified range and block until all jobs finish.
   */
  def executeMappers(startKeyPrefix: Option[KeyType],
      endKeyPrefix: Option[KeyType], mapper: Class[_],
      combiner: Option[Class[_]], nsOutput: String)
  : Unit = {
    // Determine the messages
    val startKey = startKeyPrefix.map(
        prefix => fillOutKey(prefix, newKeyInstance _)(minVal))
    val endKey = endKeyPrefix.map(
        prefix => fillOutKey(prefix, newKeyInstance _)(maxVal))
    val partitions = serversForRange(startKey, endKey)
    
    val keyClass = keyType.erasure.asInstanceOf[Class[KeyType]]
    val valueClass = valueType.erasure.asInstanceOf[Class[ValueType]]
    val keyTypeClosure = RemoteClassClosure.create(keyClass)
    val valueTypeClosure = RemoteClassClosure.create(valueClass)
    val mapperClosure = RemoteClassClosure.create(mapper)
    val combinerClosure = combiner match {
      case None => None
      case Some(c) => Some(RemoteClassClosure.create(c))
    }

    val ackFutures = (partitions zip (1 to partitions.length)).map(range => {
      val mapRequest = new MapRequest(
          range._2,
          range._1.startKey.map(serializeKey(_)),
          range._1.endKey.map(serializeKey(_)),
          keyTypeClosure, valueTypeClosure, mapperClosure, combinerClosure,
          nsOutput)
      // Range contains a list of storage nodes. We run map on the range
      // on only one storage node. Use "range.values.map(_ !! rangeRequest)"
      // to send the request to all values.
      range._1.values(0) !! mapRequest
    })
    
    // Block until all mapper finish executing.
    // TODO(rxin): fault-tolerance. If a job fails, this will wait forever.
    ackFutures.foreach( _.get() )
  }
  
  def executeReducers[ReduceKey, ReduceValue](reducer: Class[_],
                                              nsResult: String)
      (implicit reduceKeyType: Manifest[ReduceKey],
       reduceValueType: Manifest[ReduceValue]): Unit = {
    
    // Run reducers on all partitions.
    val partitions = serversForRange(None, None)
    val reducerClosure = RemoteClassClosure.create(reducer)

    val keyClass = reduceKeyType.erasure.asInstanceOf[Class[ReduceKey]]
    val valueClass = reduceValueType.erasure.asInstanceOf[Class[ReduceValue]]
    val keyTypeClosure = RemoteClassClosure.create(keyClass)
    val valueTypeClosure = RemoteClassClosure.create(valueClass)

    val ackFutures = (partitions zip (1 to partitions.length)).map(range => {
      val reduceRequest = new ReduceRequest(
          range._2,
          keyTypeClosure, valueTypeClosure, reducerClosure, nsResult)
      range._1.values(0) !! reduceRequest
    })

    // Block until all reducers finish executing.
    // TODO(rxin): fault-tolerance. If a job fails, this will wait forever.
    ackFutures.foreach( _.get() )
  }

  def executeMapReduce[ReduceKey, ReduceValue](startKeyPrefix: Option[KeyType],
      endKeyPrefix: Option[KeyType], mapper: Class[_],
      combiner: Option[Class[_]], reducer: Class[_], nsResult: String,
      deleteTemp: Boolean = false)
      (implicit reduceKeyType: Manifest[ReduceKey],
       reduceValueType: Manifest[ReduceValue]): Unit = {
    // TODO: make a unique name generator
    val nsOutput = "mapresult" + System.currentTimeMillis().toString
    val numServers = cluster.getAvailableServers.length
    // Create evenly distributed partitions over the hash space (32-bits).
    val partitionList = None :: (1 until numServers).map(
            x => Some(MapResultKey(0xffffffffL / numServers * x, None, 0, 0))
        ).toList zip cluster.getAvailableServers.map(List(_))
    val nstemp = cluster.createNamespace[MapResultKey, MapResultValue](nsOutput,
        partitionList)

    println("*****************************")
    println("***********Mappers***********")
    println("*****************************")
    var start_ms = System.currentTimeMillis()
    executeMappers(startKeyPrefix, endKeyPrefix, mapper, combiner, nsOutput)
    var elapsed_time_ms = System.currentTimeMillis() - start_ms;
    println("elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println

    println("Intermediate results distribution")
    nstemp.dumpDistribution
    println

    println("*****************************")
    println("**********Reducers***********")
    println("*****************************")
    start_ms = System.currentTimeMillis()
    nstemp.executeReducers[ReduceKey, ReduceValue](reducer, nsResult)
    elapsed_time_ms = System.currentTimeMillis() - start_ms;
    println("elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println

    if (deleteTemp) nstemp.delete
  }

}
