package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, CursorConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}
import org.apache.avro.Schema
import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._

import java.util.{ Arrays => JArrays }

import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder}

import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.mapreduce.{Mapper, MapperContext, Reducer, ReducerContext}
import edu.berkeley.cs.scads.util.{Hash}

import org.apache.avro.generic.{GenericDatumWriter}
import org.apache.avro.specific.{SpecificDatumWriter}
import scala.collection.mutable.{ListBuffer, HashMap}

/**
 * Handles a partition from [startKey, endKey). Refuses to service any
 * requests which fall out of this range, by returning a ProcessingException
 */
class PartitionHandler(
    val db: Database,
    val partitionIdLock: ZooKeeperProxy#ZooKeeperNode,
    val startKey: Option[Array[Byte]],
    val endKey: Option[Array[Byte]],
    val nsRoot: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val cluster: ScadsCluster)
    extends ServiceHandler[PartitionServiceOperation] with AvroComparator {
  protected val logger = Logger()
  implicit def toOption[A](a: A): Option[A] = Option(a)

  protected def startup() { /* No-op */ }
  protected def shutdown() {
    partitionIdLock.delete()
    db.close()
  }

  override def toString = 
    "<PartitionHandler namespace: %s, keyRange: [%s, %s)>".format(
      partitionIdLock.name, JArrays.toString(startKey.orNull), JArrays.toString(endKey.orNull))

  /**
   * True iff a particular (non-null) key is bounded by [startKey, endKey)
   */
  @inline private def isInRange(key: Array[Byte]) = true //just for the moment because of the Hash partitioning
  //TODO: I turned of the check, for the hash partitiong. Though, this is really dangerous and can cause hard to find bugs.
  //We should make the starte aware of the routing, and fix it as soon as possible.
  //  startKey.map(sk => compare(sk, key) <= 0).getOrElse(true) &&
  //  endKey.map(ek => compare(ek, key) > 0).getOrElse(true)

  /**
   * True iff startKey <= key
   */
  @inline private def isStartKeyLEQ(key: Option[Array[Byte]]) = 
    startKey.map(sk => /* If we have startKey that is not -INF */
        key.map(usrKey => /* If we have a user key that is not -INF */
          compare(sk, usrKey) <= 0) /* Both keys exist (not -INF), use compare to check range */
        .getOrElse(false)) /* startKey not -INF, but user key is -INF, so false */
    .getOrElse(true) /* startKey is -INF, so any user key works */

  /**
   * True iff endKey >= key
   */
  @inline private def isEndKeyGEQ(key: Option[Array[Byte]]) =
    endKey.map(ek => /* If we have endKey that is not +INF */
        key.map(usrKey => /* If we have a user key that is not +INF */
          compare(ek, usrKey) >= 0) /* Both keys exist (not +INF), use compare to check range */
        .getOrElse(false)) /* endKey not +INF, but user key is +INF, so false */
    .getOrElse(true) /* startKey is +INF, so any user key works */

  // Helper function to compare two Arrays.  Returns true if equal.
  @inline private def arrayEquals(a: Array[_], b: Array[_]): Boolean = {
    if (a.length != b.length)
      return false
    val len = a.length
    for (i <- (0 until len)) {
      if (a(i) != b(i))
        return false
    }
    return true
  }

  @inline private def encodeData(writer: SpecificDatumWriter[AvroRecord],
      obj: AvroRecord) = {
    val baos = new java.io.ByteArrayOutputStream
    val enc  = new BinaryEncoder(baos)
    writer.write(obj, enc)
    baos.toByteArray
  }

  // Objects for serializing/deserializing
  protected lazy val decoderFactory = (new DecoderFactory).configureDirectDecoder(true)

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    // key validation
    val (keysInRange, keysInQuestion) = msg match {
      /* GetRequest key must be bounded by [startKey, endKey) */
      case GetRequest(key) => (isInRange(key), Left(key))
      /* PutRequest key must be bounded by [startKey, endKey) */
      case PutRequest(key, _) => (isInRange(key), Left(key))
      /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
       * entire range is allowed */
      case GetRangeRequest(minKey, maxKey, _, _, _) =>
        (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
      /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
       * entire range is allowed */
      case MapRequest(_, minKey, maxKey, _, _, _, _, _) =>
        (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
      /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
       * entire range is allowed */
      case CountRangeRequest(minKey, maxKey) =>
        (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
      /* TestSetRequest key must be bounded by [startKey, endKey) */
      case TestSetRequest(key, _, _) => (isInRange(key), Left(key))
      case _ => (true, null)
    }

    if (keysInRange) {
      /** Invariant: All keys as input from client are valid for this
       * partition */
      msg match {
        case GetRequest(key) => {
          val (dbeKey, dbeValue) = (new DatabaseEntry(key), new DatabaseEntry)
          db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
          reply(GetResponse(Option(dbeValue.getData())))
        }
        case PutRequest(key, value) => {
          value match {
            case Some(v) => db.put(null, new DatabaseEntry(key), new DatabaseEntry(v))
            case None => db.delete(null, new DatabaseEntry(key))
          }
          reply(PutResponse())
        }
        case BulkPutRequest(records) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          records.foreach(rec => db.put(txn, new DatabaseEntry(rec.key), new DatabaseEntry(rec.value.get)))
          try {
            txn.commit()
            reply(BulkPutResponse())
          } catch {
            case e: Exception =>
              logger.error(e, "Could not commit BulkPutRequest")
              reply(ProcessingException(e.getMessage, e.getStackTrace.mkString("\n")))
          }
        }
        case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
          logger.debug("[%s] GetRangeRequest: [%s, %s)]", this, JArrays.toString(minKey.orNull), JArrays.toString(maxKey.orNull))
          val records = new scala.collection.mutable.ListBuffer[Record]
          iterateOverRange(minKey, maxKey, limit, offset, ascending)((key, value, _) => {
            records += Record(key.getData, value.getData)
          })
          reply(GetRangeResponse(records))
        }
        case BatchRequest(ranges) => {
          val results = new scala.collection.mutable.ListBuffer[GetRangeResponse]
          ranges.foreach {
            case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
              val records = new scala.collection.mutable.ListBuffer[Record]
              iterateOverRange(minKey, maxKey, limit, offset, ascending)((key, value, _) => {
                records += Record(key.getData, value.getData)
              })
              results += GetRangeResponse(records)
            }
            case _ => throw new RuntimeException("BatchRequests only implemented for GetRange")
          }
          reply(BatchResponse(results))
        }
        case MapRequest(mapperId, minKey, maxKey, keyTypeClosure,
                        valueTypeClosure, mapperClosure, combinerClosure,
                        nsOutput) => {
          // TODO(rxin): This part of the code should be abstracted out to
          // the mapreduce folder.
          logger.debug("[%s] MapRequest: [%s, %s)]", this,
                       JArrays.toString(minKey.orNull),
                       JArrays.toString(maxKey.orNull))

          // Initialize mapper and context ... Perhaps do more setup here.
          val mapperClass = mapperClosure.retrieveClass()
          val mapper = mapperClass.newInstance().asInstanceOf[ Mapper ]
          val context = new MapperContext(cluster)
          
          // keySchema is given. Let's get the valueSchema.
          //val keyTypeClass = keyTypeClosure.retrieveClass()
          //val keyInstance = keyTypeClass.newInstance()
          val valueTypeClass = valueTypeClosure.retrieveClass()
          
          val keyReader = new SpecificDatumReader[SpecificRecordBase](keySchema)
          val valueReader = new SpecificDatumReader[SpecificRecordBase](
              valueTypeClass.newInstance().asInstanceOf[AvroRecord].getSchema)

          var batchList = List[(AvroRecord, AvroRecord)]()
          var batchLength = 0
          iterateOverRange(minKey, maxKey)((key, value, _) => {
            // TODO(rxin): reuse decoder (replace the 3rd argument null).
            val keyObj = keyReader.read(null,
                decoderFactory.createBinaryDecoder(key.getData, null))
                .asInstanceOf[AvroRecord]
            val valueBinaryData = value.getData.slice(16, value.getData.length)
            val valueObj = valueReader.read(null,
                decoderFactory.createBinaryDecoder(valueBinaryData, null))
                .asInstanceOf[AvroRecord]
            batchList = Tuple2(keyObj, valueObj) :: batchList
            batchLength += 1
            if (batchLength > 25) {
              mapper.map(batchList, context)
              batchList = List[(AvroRecord, AvroRecord)]()
              batchLength = 0
            }
          })
          if (batchLength > 0) {
            mapper.map(batchList, context)
          }

          val resultContext = combinerClosure match {
            case None => context
            case Some(c) => {
              val combiner = c.retrieveClass().newInstance().asInstanceOf[Reducer]
              val combinerContext = new ReducerContext(cluster)
              context.output.foreach(t => {
                combiner.reduce(t._1, t._2, combinerContext)
              })
              combinerContext
            }
          }

          if (resultContext.output.size > 0) {
            val ns = cluster.getNamespace[MapResultKey, MapResultValue](nsOutput)
            val keyWriter = new SpecificDatumWriter[AvroRecord](
                resultContext.output.head._1.getSchema)
            val valueWriter = new SpecificDatumWriter[AvroRecord](
                resultContext.output.head._2.head.getSchema)

            var resultList = List[(MapResultKey, MapResultValue)]()
            var resultListLength = 0
            resultContext.output.foreach(t => {
              val serializedKey = encodeData(keyWriter, t._1)
              (t._2 zip (1 to t._2.length)).foreach(v => {
                 val serializedValue = encodeData(valueWriter, v._1)
                 resultList = Tuple2(MapResultKey(Hash.hashMurmur2(serializedKey),
                                                  Some(serializedKey),
                                                  mapperId, v._2),
                                     MapResultValue(serializedValue)) :: resultList
                 resultListLength += 1
                 if (resultListLength > 1000) {
                   ns ++= resultList
                   resultList = List[(MapResultKey, MapResultValue)]()
                   resultListLength = 0
                 }
              })
            })
            if (resultListLength > 0) {
               ns ++= resultList
             }
          }

          // Reply with an ACK when done.
          reply(MapRequestComplete())
        }
        case ReduceRequest(reducerId, keyTypeClosure,
                           valueTypeClosure, reducerClosure, nsResult) => {
          // Initialize reducer and context ... Perhaps do more setup here.
          val reducerClass = reducerClosure.retrieveClass()
          val reducer = reducerClass.newInstance().asInstanceOf[ Reducer ]
          val context = new ReducerContext(cluster)

          val keyReader = new SpecificDatumReader[MapResultKey](
              classOf[MapResultKey].newInstance.getSchema)
          val valueReader = new SpecificDatumReader[MapResultValue](
              classOf[MapResultValue].newInstance.getSchema)

          val keyTypeClass = keyTypeClosure.retrieveClass()
          val valueTypeClass = valueTypeClosure.retrieveClass()

          val userKeyReader = new SpecificDatumReader[SpecificRecordBase](
              keyTypeClass.newInstance.asInstanceOf[AvroRecord].getSchema)
          val userValueReader = new SpecificDatumReader[SpecificRecordBase](
              valueTypeClass.newInstance.asInstanceOf[AvroRecord].getSchema)

          // Stores the values for the current key.
          val valueList = ListBuffer[AvroRecord]()
          // Saves the current key, to compare with.  Save both the byte array
          // (to compare), and the object (to reduce).
          var oldKey = (Array[Byte](),
                        keyTypeClass.newInstance.asInstanceOf[AvroRecord])
          iterateOverRange(None, None)((key, value, _) => {
            // TODO(rxin): reuse decoder (replace the 3rd argument null).
            val keyObj = keyReader.read(null,
                decoderFactory.createBinaryDecoder(key.getData, null))
            val valueData = value.getData.slice(16, value.getData.length)
            val valueObj = valueReader.read(null,
                decoderFactory.createBinaryDecoder(valueData, null))

            // bytes is an Option, but it will always be set by the mapper.
            val keyObj_bytes = keyObj.bytes.get
            val userKeyObj = userKeyReader.read(null,
                decoderFactory.createBinaryDecoder(keyObj_bytes, null))
                .asInstanceOf[AvroRecord]
            val userValueObj = userValueReader.read(null,
                decoderFactory.createBinaryDecoder(valueObj.bytes, null))
                .asInstanceOf[AvroRecord]

            if (valueList.length == 0) {
              // no values inserted yet.
              oldKey = (keyObj_bytes.clone, userKeyObj)
              valueList.append(userValueObj)
            } else {
              if (arrayEquals(keyObj_bytes, oldKey._1)) {
                // Same key.
                valueList.append(userValueObj)
              } else {
                // A different key, run the reducer on all the values of the
                // previous key.
                reducer.reduce(oldKey._2, valueList, context)

                valueList.clear()
                oldKey = (keyObj_bytes.clone, userKeyObj)
                valueList.append(userValueObj)
              }
            }
          })
          if (valueList.length != 0) {
            // run the reducer for the last set of values.
            reducer.reduce(oldKey._2, valueList, context)
          }

          if (context.output.size > 0) {
            val outputKeySchema = context.output.head._1.getSchema
            val outputValueSchema = context.output.head._2.head.getSchema
            val ns = cluster.getNamespace(nsResult, outputKeySchema,
                                          outputValueSchema)
            var resultList = List[(IndexedRecord, IndexedRecord)]()
            var resultListLength = 0
            context.output.foreach(t => {
              resultList = Tuple2(t._1.asInstanceOf[IndexedRecord],
                                  t._2.head.asInstanceOf[IndexedRecord]) :: resultList
              resultListLength += 1
              if (resultListLength > 1000) {
                // write the batch out
                ns ++= resultList
                resultList = List[(IndexedRecord, IndexedRecord)]()
                resultListLength = 0
              }
            })
            if (resultListLength > 0) {
              ns ++= resultList
            }
          }

          reply(ReduceRequestComplete())
        }
        case CountRangeRequest(minKey, maxKey) => {
          var count = 0
          iterateOverRange(minKey, maxKey)((_,_,_) => count += 1)
          reply(CountRangeResponse(count))
        }
        case TestSetRequest(key, value, expectedValue) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          val dbeKey = new DatabaseEntry(key)
          val dbeCurrentValue = new DatabaseEntry
          db.get(txn, dbeKey, dbeCurrentValue, LockMode.READ_COMMITTED)
          if(JArrays.equals(expectedValue.orNull, dbeCurrentValue.getData)){
            value match {
              case Some(v) => db.put(txn, dbeKey, new DatabaseEntry(v))
              case None => db.delete(txn, dbeKey)
            }
            txn.commit()
            reply(TestSetResponse(true))
          } else {
            txn.abort()
            reply(TestSetResponse(false))
          }
        }
        case CopyDataRequest(src, overwrite) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          val dbeExistingValue = new DatabaseEntry
          val dbeKey = new DatabaseEntry
          val dbeValue = new DatabaseEntry
          logger.debug("Opening iterator for data copy")
          val iter = new PartitionIterator(src, startKey, endKey)

          logger.debug("Begining copy")
          iter.foreach(rec => {
            dbeKey.setData(rec.key); dbeValue.setData(rec.value.get)
            if(overwrite == true) {
              db.put(txn, dbeKey, dbeValue)
            }
            else {
              if(db.get(txn, dbeKey, dbeExistingValue, LockMode.READ_COMMITTED) != OperationStatus.SUCCESS)
                db.put(txn, dbeKey, dbeValue)
            }
          })
          logger.debug("Copy complete.  Begining commit")
          txn.commit()
          logger.debug("Comit complete")
          reply(CopyDataResponse())
        }
        case GetResponsibilityRequest() => {
          reply(GetResponsibilityResponse(startKey, endKey))
        }
        case _ => src.foreach(_ ! ProcessingException("Not Implemented", ""))
      }
    } else {
      val errorMsg = keysInQuestion match {
        case Left(key) =>
          "Expected a key in range [%s, %s), but got %s".format(
            JArrays.toString(startKey.orNull), 
            JArrays.toString(endKey.orNull), 
            JArrays.toString(key))
        case Right((minKey, maxKey)) =>
          "Expected a range bounded (inclusively) by [%s, %s), but got [%s, %s)".format(
            JArrays.toString(startKey.orNull),
            JArrays.toString(endKey.orNull),
            JArrays.toString(minKey.orNull),
            JArrays.toString(maxKey.orNull))
      }
      logger.error("Received errorneous request %s. Error was: %s", msg, errorMsg)
      reply(RequestRejected("Key(s) are out of range: %s".format(errorMsg), msg))
    }
  }

  /**
   * Delete [startKey, endKey)
   */
  def deleteEntireRange(txn: Option[Transaction]) { deleteRange(startKey, endKey, txn) }

  /**
   * Delete [lowerKey, upperKey)
   */
  def deleteRange(lowerKey: Option[Array[Byte]], upperKey: Option[Array[Byte]], txn: Option[Transaction]) {
    assert(isStartKeyLEQ(lowerKey) && isEndKeyGEQ(upperKey), "startKey <= lowerKey && endKey >= upperKey required")
    iterateOverRange(lowerKey, upperKey, txn = txn)((_, _, cursor) => {
      cursor.delete()
    })
  }

  /**
   * Low level method to iterate over a given range on the database. it is up
   * to the caller to validate minKey and maxKey. The range iterated over is
   * [minKey, maxKey), with the order specified by ascending (and limits
   * respected)
   */
  private def iterateOverRange(minKey: Option[Array[Byte]], 
                               maxKey: Option[Array[Byte]], 
                               limit: Option[Int] = None, 
                               offset: Option[Int] = None, 
                               ascending: Boolean = true, 
                               txn: Option[Transaction] = None)
      (func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit): Unit = {
    val (dbeKey, dbeValue) = (new DatabaseEntry, new DatabaseEntry)
    val cur = db.openCursor(txn.orNull, CursorConfig.READ_UNCOMMITTED)

    var status: OperationStatus = (ascending, minKey, maxKey) match {
      case (true, None, _) => cur.getFirst(dbeKey, dbeValue, null)
      case (true, Some(startKey), _) => cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null)
      case (false, _, None) => cur.getLast(dbeKey, dbeValue, null)
      case (false, _, Some(startKey)) => {
        // Check if maxKey is past the last key in the database, if so start from the end
        if(cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null) == OperationStatus.NOTFOUND)
          // no need to skip back one since the maxKey was not found anyways
          cur.getLast(dbeKey, dbeValue, null)
        else {
          // need to check that the cursor is pointing to the first key that
          // is LESS THAN maxKey. getSearchKeyRange semantics only guarantee
          // that the cursor is pointing to the smallest key >= maxKey
          var status = cur.getCurrent(dbeKey, dbeValue, null)
          if(status == OperationStatus.SUCCESS && compare(startKey, dbeKey.getData) <= 0)
            status = cur.getPrev(dbeKey, dbeValue, null)
          status
        }
      }
    }

    var toSkip = offset.getOrElse(0)
    var returnedCount = 0
    if (status != OperationStatus.SUCCESS) {
      cur.close()
      return
    }

    if(ascending) {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getNext(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      if (status == OperationStatus.SUCCESS)
        status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            limit.map(_ > returnedCount).getOrElse(true) &&
            maxKey.map(mk => compare(dbeKey.getData, mk) < 0 /* Exclude maxKey from range */).getOrElse(true)) {
        func(dbeKey, dbeValue, cur)
        returnedCount += 1
        status = cur.getNext(dbeKey, dbeValue, null)
      }
    }
    else {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getPrev(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      if (status == OperationStatus.SUCCESS)
        status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            limit.map(_ > returnedCount).getOrElse(true) &&
            minKey.map(compare(_, dbeKey.getData) <= 0).getOrElse(true)) {
        func(dbeKey, dbeValue,cur)
        returnedCount += 1
        status = cur.getPrev(dbeKey, dbeValue, null)
      }
    }
    cur.close()
  }
}
