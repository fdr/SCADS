package edu.berkeley.cs.scads.util

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

import edu.berkeley.cs.scads.mapreduce.{Mapper, MapperContext, Reducer, ReducerContext}

case class IntSeqRec(var f1: List[Int]) extends AvroRecord

// mapper, combiner, reducer for building index for characters.
class CharIndexMapper extends Mapper {
  def map(data: Seq[(AvroRecord, AvroRecord)], context: MapperContext) = {
    data.foreach {
      case (key, value) =>
      val keyInt = key.asInstanceOf[IntRec].f1
      val valueStr = value.asInstanceOf[StringRec].f1
      // Output (char, rowid)
      valueStr.foreach(c => context.collect(StringRec(c.toString.toLowerCase),
                                            IntRec(keyInt)))
    }
  }
}
class CharIndexCombiner extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    val keyStr = key.asInstanceOf[StringRec]
    val list = values.map(x => x.asInstanceOf[IntRec].f1)
        .foldLeft(List[Int]())((l, i) => i::l)
    context.collect(keyStr, IntSeqRec(list))
  }
}
class CharIndexReducer extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    val keyStr = key.asInstanceOf[StringRec]
    val list = values.map(x => x.asInstanceOf[IntSeqRec].f1)
        .foldLeft(List[Int]())((l, i) => i:::l)
    context.collect(keyStr, IntSeqRec(list))
  }
}

// mapper, reducer for character counting.
class CharCounterMapper extends Mapper {
  def map(data: Seq[(AvroRecord, AvroRecord)], context: MapperContext) = {
    data.foreach {
      case (key, value) =>
      val keyInt = key.asInstanceOf[IntRec].f1
      val valueStr = value.asInstanceOf[StringRec].f1
      // Output (char, rowid)
      valueStr.foreach(c => context.collect(StringRec(c.toString.toLowerCase),
                                            IntRec(keyInt)))
    }
  }
}
class CharCounterReducer extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    val keyStr = key.asInstanceOf[StringRec]
    val sum = values.foldLeft(0)((x, y) => x + y.asInstanceOf[IntRec].f1)
    context.collect(keyStr, IntRec(sum))
  }
}

class TestScadsMapper {
  def run() {
    // Creates a local client
    val client = TestScalaEngine.newScadsCluster(2)
    val storageServer = client.getAvailableServers

    // Creates a partition and replicates it across all three storage servers
    val ns1 = client.createNamespace[IntRec, StringRec](
        "getputtest",
        List((None, List(storageServer(0))),
             (IntRec(500), List(storageServer(1)))))

    println("*****************************")
    println("********Loading Data*********")
    println("*****************************")
    
    // Writes data
    (1 to 1000).foreach(index => ns1.put(IntRec(index),
                                         StringRec("VAL" + index)))

    // Fill the test lookup table.
    val ns2 = client.createNamespace[IntRec, StringRec](
        "lookuptest",
        List((None, List(storageServer(0))),
             (IntRec(50), List(storageServer(1)))))
    (1 to 100).foreach(index => ns2.put(IntRec(index),
                                        StringRec("LOOKUP" + (index % 3 + 17))))

    val ns3 = client.getNamespace[IntRec, StringRec]("getputtest")

    // Create output namespace
    // This output is for character counting map/reduce.
    //val nsOutput = client.createNamespace[StringRec, IntRec](
    //    "reduceResultCounting",
    //    List((None, List(storageServer(0))),
    //         (StringRec("m"), List(storageServer(1)))))
           
    // This output is for the character index map/reduce.
    val nsOutput = client.createNamespace[StringRec, IntSeqRec](
        "reduceResultIndex",
        List((None, List(storageServer(0))),
             (StringRec("m"), List(storageServer(1)))))

    val start_ms = System.currentTimeMillis()
    // character counting map/reduce USING combiner.
    //ns3.executeMapReduce[StringRec, IntRec](
    //    None, None, classOf[CharCounterMapper],
    //    Some(classOf[CharCounterReducer]), classOf[CharCounterReducer],
    //    "reduceResultCounting")

    // character counting map/reduce NOT USING combiner.
    // (map output is large, to utilize bulk put)
    //ns3.executeMapReduce[StringRec, IntRec](
    //    None, None, classOf[CharCounterMapper], None,
    //    classOf[CharCounterReducer], "reduceResultCounting")

    // character index building map/reduce.
    ns3.executeMapReduce[StringRec, IntSeqRec](
        None, None, classOf[CharIndexMapper],
        Some(classOf[CharIndexCombiner]), classOf[CharIndexReducer],
        "reduceResultIndex")
    val elapsed_time_ms = System.currentTimeMillis() - start_ms;

    println("*****************************")
    println("********Final Result*********")
    println("*****************************")
    nsOutput.getRange(None, None).foreach(t => println(t))

    println("\ntotal elapsed time (sec): " + elapsed_time_ms / 1000.0)
  }
}

object TestScadsMapper {
  def main(args: Array[String]) {
    val test = new TestScadsMapper()
    test.run()
    println("Exiting...")
    System.exit(0)
  }
}
