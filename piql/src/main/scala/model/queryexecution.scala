package edu.berkeley.cs.scads.piql

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import edu.berkeley.cs.scads.piql.parser.{BoundValue, BoundIntegerValue, BoundStringValue, BoundFixedValue}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.generic.{IndexedRecord, GenericData}
import edu.berkeley.cs.scads.storage.Namespace
import edu.berkeley.Log2

abstract sealed class JoinCondition
case class AttributeCondition(attrName: String) extends JoinCondition
case class BoundValueLiteralCondition[T](fieldValue: BoundFixedValue[T]) extends JoinCondition

case class EntityClass(name: String)

/* Query Plan Nodes */
abstract class QueryPlan
abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: List[BoundValue]) extends TupleProvider
case class PrefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, conditions: Seq[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, conditions: Seq[JoinCondition], child: EntityProvider) extends TupleProvider
case class Materialize(entityType: EntityClass, child: TupleProvider) extends EntityProvider
case class Selection(equalityMap: HashMap[String, BoundValue], child: EntityProvider) extends EntityProvider
case class Sort(fields: List[String], ascending: Boolean, child: EntityProvider) extends EntityProvider
case class TopK(k: BoundValue, child: EntityProvider) extends EntityProvider

class Environment {
  var namespaces: Map[String, Namespace[SpecificRecordBase, SpecificRecordBase]] = null
}

object QueryExecutor {
	val opLogger = Logger.getLogger("scads.queryexecution.operators")
}

abstract trait QueryExecutor {
	val qLogger = Logger.getLogger("scads.queryexecution")
	
	/* Type Definitions */
	type TupleStream = Seq[(SpecificRecordBase, SpecificRecordBase)]
	type EntityStream = Seq[Entity[_,_]]

  implicit def toBoundInt(i: Int) = BoundIntegerValue(i)
  implicit def toBoundString(s: String) = BoundStringValue(s)

	/* Tuple Providers */
  protected def singleGet(namespace: String, key: List[BoundValue])(implicit env: Environment): TupleStream = {
	val start = System.nanoTime()

	// Start op body
    qLogger.debug("test")
    Log2.debug(qLogger, "singleGet", namespace, key)

    val ns = env.namespaces(namespace)
    val keyRec = ns.keyClass.newInstance()
    key.zipWithIndex.foreach {
      case(v: BoundFixedValue[_], idx: Int) => keyRec.put(idx, v.value)
    }

    val result = ns.get(keyRec) match {
      case Some(v) => List((keyRec, v))
      case None => Nil
    }

    Log2.debug(qLogger, "singleGet Result:", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("singleGet(" + key + "), start=" + (start/denom) + ", end=" + (end/denom) + ", latency=" + ((end-start)/denom))
	//QueryExecutor.opLogger.info("singleGet(" + key + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

  //TODO: Use limit/ascending parameters
	protected def prefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean)(implicit env: Environment): TupleStream = {
	val start = System.nanoTime()
	
	// Start op body
    Log2.debug(qLogger, "prefixGet", namespace, prefix, limit, boolean2Boolean(ascending))
    val ns = env.namespaces(namespace)
    val key = ns.keyClass.newInstance()
    prefix.zipWithIndex.foreach {
      case (value: BoundFixedValue[_], idx: Int) => key.put(idx, value.value)
    }
    val result = ns.getPrefix(key, prefix.length)
    Log2.debug(qLogger, "prefixGet result:", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("prefixGet(" + prefix + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

  //TODO: Deal with values that return None
	protected def sequentialDereferenceIndex(targetNamespace: String, child: TupleStream)(implicit env: Environment): TupleStream = {
	val start = System.nanoTime()
	
	// Start op body
    Log2.debug(qLogger, "sequentialDereferenceIndex", targetNamespace, child)
    val ns = env.namespaces(targetNamespace)
    val result = child.map(c => (c._2, ns.get(c._2).get))
    Log2.debug(qLogger, "sequentialDereferenceIndex result:", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("sequentialDereferenceIndex(" + targetNamespace + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

  private def mkKey(keyClass: Class[EntityPart], conditions: List[JoinCondition], entity: Entity[_,_]): SpecificRecordBase = {
    val key = keyClass.newInstance()
    val keyParts = conditions.flatMap {
      case AttributeCondition(attrName) => entity.get(attrName) match {
        case ep: EntityPart => ep.flatValues
        case value => List(value)
      }
      case bv: BoundValueLiteralCondition[_] => List(bv.fieldValue.value)
    }

    println("keyParts:" + keyParts)

    keyParts.zipWithIndex.foreach {
      case(v: Any, idx: Int) => {println("setting " + idx + " " + v); key.flatPut(idx, v)}
    }
    return key
  }

  //TODO: use limit / ascending parameters
  //TODO: parallelize
	protected def prefixJoin(namespace: String, conditions: List[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityStream)(implicit env: Environment): TupleStream = {
	val start = System.nanoTime()

	// Start op body
    Log2.debug(qLogger, "prefixJoin", namespace, conditions, limit, boolean2Boolean(ascending), child)
    val ns = env.namespaces(namespace)
    val result = child.flatMap(c => {
      val key = mkKey(ns.keyClass.asInstanceOf[Class[EntityPart]], conditions, c)
      Log2.debug(qLogger, "prefixJoin doing get on key: ", key)
      ns.getPrefix(key, conditions.length)
    })
    Log2.debug(qLogger, "prefixJoin result: ", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("prefixJoin(" + namespace + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

	protected def pointerJoin(namespace: String, conditions: List[JoinCondition], child: EntityStream)(implicit env: Environment): TupleStream = {
	val start = System.nanoTime()

	// Start op body
    Log2.debug(qLogger, "pointerJoin", namespace, conditions, child)
    val ns = env.namespaces(namespace)
    val result = child.map(c => {
      val key = mkKey(ns.keyClass.asInstanceOf[Class[EntityPart]], conditions, c)

      Log2.debug(qLogger, "pointerJoin doing get on key: ", key)

      println("pointerJoin doing get on key: " + key)	// debugging
		
      (key, ns.get(key).get)
    })
    Log2.debug(qLogger, "pointerJoin result:", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("pointerJoin(" + namespace + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

	/* Entity Providers */
	protected def materialize(entityClass: Class[Entity[_,_]], child: TupleStream)(implicit env: Environment): EntityStream = {
		val start = System.nanoTime()

		// Start op body
    Log2.debug(qLogger, "materialize", entityClass, child)
    val result = child.map(c => {
      val e = entityClass.newInstance().asInstanceOf[Entity[SpecificRecordBase,SpecificRecordBase]]
      e.key.parse(c._1.toBytes)
      e.value.parse(c._2.toBytes)
      e
    })
    Log2.debug(qLogger, "materialize result:", result)
	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("materialize(" + entityClass + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

	protected def selection(equalityMap: HashMap[String, BoundValue], child: EntityStream): EntityStream = {
		val start = System.nanoTime()

		// Start op body
    Log2.debug(qLogger, "selection", equalityMap, child)
    val result = child.filter(c => {
      equalityMap.map {case (attrName: String, bv: BoundFixedValue[_]) => c.get(attrName) equals bv.value}.reduceLeft(_&_)
    })
    Log2.debug(qLogger, "selection result: ", result)
 	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("selection, start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

   return result
  }

	protected def sort(fields: List[String], ascending: Boolean, child: EntityStream): EntityStream = {
		val start = System.nanoTime()

		// Start op body
    Log2.debug(qLogger, "sort", fields, boolean2Boolean(ascending), child)
    val comparator = (a: Entity[_,_], b: Entity[_,_]) => {
      fields.map(f => (a.get(f), b.get(f)) match {
        case (x: Integer, y: Integer) => x.intValue() < y.intValue()
        case (x: String, y: String) => x < y
      }).reduceLeft(_&_)
    }

    val ret = child.toArray
    scala.util.Sorting.stableSort(ret, comparator)
    val result = if(ascending) ret else ret.reverse
    Log2.debug(qLogger, "sort result:", result)
 	// End op body

	val end = System.nanoTime()
	val denom = 1000000.0	// want to report start/end times and latency in ms
	QueryExecutor.opLogger.info("sort(" + fields.length + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)

    return result
  }

	protected def topK(k: BoundIntegerValue, child: EntityStream): EntityStream = {
		val start = System.nanoTime()

		// Start op body
		val res = child.take(k.value)
	 	// End op body

		val end = System.nanoTime()
		val denom = 1000000.0	// want to report start/end times and latency in ms
		QueryExecutor.opLogger.info("topK(" + k + "), start=" + start/denom + ", end=" + end/denom + ", latency=" + (end-start)/denom)
		
		res
	}
}
