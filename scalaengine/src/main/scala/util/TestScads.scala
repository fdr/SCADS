package edu.berkeley.cs.scads.util

import scala.collection.mutable._
import scala.io.Source

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

import Jama._

case class TestStringRec(var f1: String) extends AvroRecord
case class TestIntStringRec(var f1: Int, var f2: String) extends AvroRecord

case class CovtypeId(var id: Int) extends AvroRecord
case class CovtypeFeatures(var features: String) extends AvroRecord

object CovtypeLoader {
  // http://archive.ics.uci.edu/ml/datasets/Covertype
  // Increase heapsize in jrun to load all the data.
//  val filename = "../../data_covtype/covtype.data" 
  // head -n 100000 covtype.data > covtype.data.small
  val filename = "../../data_covtype/covtype.data.small" 
  var id = 0

  def transformLine(line: String): (CovtypeId, CovtypeFeatures) = {
    id += 1
    (CovtypeId(id), CovtypeFeatures(line))
  }

  def loadData(client: ScadsCluster, ns_name: String) = {
    val start_ms = System.currentTimeMillis()
    
    // Retrieves the namespace from the client2 perspective
    val ns = client.getNamespace[CovtypeId, CovtypeFeatures](ns_name)

    // This massive bulk put does NOT work...
//    ns ++= Source.fromFile(filename).getLines().map(transformLine).toIterable

    // Insert in batches.
    var total_count = 0
    var batch_list = ListBuffer[(CovtypeId, CovtypeFeatures)]()
    for (line <- Source.fromFile(filename).getLines()) {
      batch_list += (transformLine(line))
      if (batch_list.length >= 1000) {
        ns ++= batch_list
        total_count += batch_list.length
        println("inserted records: " + total_count)
        batch_list.clear()
      }
    }
    if (batch_list.length > 0) {
      ns ++= batch_list
      total_count += batch_list.length
      println("inserted records: " + total_count)
      batch_list.clear()
    }

    val load_time_ms = System.currentTimeMillis() - start_ms;
    println("load time (sec): " + load_time_ms / 1000.0)
  }
}

class TestScads {

  def TestCovtype() {
    // Creates local storage servers.
    val storageHandler = TestScalaEngine.getTestHandler(20)

    // Creates a local client.
    val client1 = new ScadsCluster(storageHandler.head.root)
    val storageServer = client1.getAvailableServers

    val partition_size = 5000
//    val partition_size = 29050

    // Evenly distributes partitions.
    val ns1 =
        client1.createNamespace[CovtypeId, CovtypeFeatures]("covtypetest",
            List((None, List(storageServer(0))),
                 (CovtypeId(partition_size*1), List(storageServer(1))),
                 (CovtypeId(partition_size*2), List(storageServer(2))),
                 (CovtypeId(partition_size*3), List(storageServer(3))),
                 (CovtypeId(partition_size*4), List(storageServer(4))),
                 (CovtypeId(partition_size*5), List(storageServer(5))),
                 (CovtypeId(partition_size*6), List(storageServer(6))),
                 (CovtypeId(partition_size*7), List(storageServer(7))),
                 (CovtypeId(partition_size*8), List(storageServer(8))),
                 (CovtypeId(partition_size*9), List(storageServer(9))),
                 (CovtypeId(partition_size*10), List(storageServer(10))),
                 (CovtypeId(partition_size*11), List(storageServer(11))),
                 (CovtypeId(partition_size*12), List(storageServer(12))),
                 (CovtypeId(partition_size*13), List(storageServer(13))),
                 (CovtypeId(partition_size*14), List(storageServer(14))),
                 (CovtypeId(partition_size*15), List(storageServer(15))),
                 (CovtypeId(partition_size*16), List(storageServer(16))),
                 (CovtypeId(partition_size*17), List(storageServer(17))),
                 (CovtypeId(partition_size*18), List(storageServer(18))),
                 (CovtypeId(partition_size*19), List(storageServer(19)))
               ))

    CovtypeLoader.loadData(client1, "covtypetest")

    val transform_array = (d: Array[Double]) => {
      val transformed_array = new Array[Double](13)
      for (i <- (0 until transformed_array.length)) {
        transformed_array.update(i, d(i))
      }
      var position = 1
      // 10-13 is the wilderness type, indicator variable.
      for (i <- (10 until 14)) {
        if (d(i) != 0) {
          position = i - 9
        }
      }
      transformed_array.update(10, position)
      // 14-53 is the soil type, indicator variable.
      for (i <- (14 until 54)) {
        if (d(i) != 0) {
          position = i - 13
        }
      }
      transformed_array.update(11, position)
      transformed_array.update(12, 1)
      transformed_array
    }

    // Map function.  Converts all features to a list of doubles and appends 1
    // to the vector for the constant parameter.  Drops the last column because
    // that is the output column.
    val mapFn = (k: CovtypeId, v: CovtypeFeatures) => {
      val double_array = v.features.split(",").map(x => x.toDouble)
      val transformed_array = transform_array(double_array)
      new Matrix(transformed_array, transformed_array.length)
    }

    // Aggregate function.  Vector multiplies each feature vector with itself
    // to generate a matrix.  Then adds to the accumulated results.
    val aggFn = (m: Matrix, aggOpt: Option[Matrix]) => {
      aggOpt match {
        case None => m.times(m.transpose)
        case Some(mat) => mat.plusEquals(m.times(m.transpose))
      }
    }

    // Map and aggregate all the data.
    var start_ms = System.currentTimeMillis()
    val results = ns1.mapAggRange[Matrix, Matrix](None, None, mapFn, Some(aggFn))
    val agg_results = results.reduceLeft((m1, m2) => m1.plusEquals(m2))
    var map_time_ms = System.currentTimeMillis() - start_ms;
    agg_results.print(10, 5)
    println("map time (sec): " + map_time_ms / 1000.0)

    // Map function for the second part of the algorithm.  Converts the features
    // to a list of doubles and scales it by the output value (last column).
    val map2Fn = (k: CovtypeId, v: CovtypeFeatures) => {
      val double_array = v.features.split(",").map(x => x.toDouble)
      val transformed_array = transform_array(double_array)
      val scale = double_array(double_array.length - 1)
      val m = new Matrix(transformed_array, transformed_array.length)
      m.timesEquals(scale)
    }

    // Aggregate function for second part.  Simply add to the accumulated results.
    val agg2Fn = (v: Matrix, aggOpt: Option[Matrix]) => {
      aggOpt match {
        case None => v
        case Some(agg) => agg.plusEquals(v)
      }
    }

    start_ms = System.currentTimeMillis()
    val results2 = ns1.mapAggRange[Matrix, Matrix](None, None, map2Fn, Some(agg2Fn))
    val agg_results2 = results2.reduceLeft((m1, m2) => m1.plusEquals(m2))
    map_time_ms = System.currentTimeMillis() - start_ms;
    agg_results2.print(10, 5)
    println("map2 time (sec): " + map_time_ms / 1000.0)

    val final_result = agg_results.solve(agg_results2)
    final_result.print(10, 5)
  }

  def run() {
    // Creates 3 local storage server
    val storageHandler = TestScalaEngine.getTestHandler(2)

    // Creates a local client
    val client1 = new ScadsCluster(storageHandler.head.root)
    val client2 = new ScadsCluster(storageHandler.head.root)
    val storageServer = client1.getAvailableServers

    // Creates a partition and replicates it across all three storage servers
    val ns1 =
        client1.createNamespace[IntRec, TestStringRec]("getputtest",
            List((None, List(storageServer(0))),
                 (IntRec(50), List(storageServer(1)))))

    // Retrieves the namespace from the client2 perspective
    val ns2 = client2.getNamespace[IntRec, TestStringRec]("getputtest")

    // Writes data
    (1 to 100).foreach(index => ns1.put(IntRec(index),
                                        TestStringRec("VAL" + index)))

    val mapFn = (k: IntRec, v: TestStringRec) => {
      TestIntStringRec(k.f1, v.f1)
    }

    val aggFn = (v: TestIntStringRec, agg: Option[IntRec]) => {
      IntRec(agg.getOrElse(IntRec(0)).f1 + v.f1)
    }

    // Reads the keys
    ns1.mapRange[TestIntStringRec](IntRec(40), IntRec(60), mapFn).foreach(pair => println(pair))
    ns1.mapAggRange[TestIntStringRec, IntRec](IntRec(40), IntRec(60), mapFn, Some(aggFn)).foreach(pair => println(pair))
  }
}

object TestScads {
  def main(args: Array[String]) {
    val test = new TestScads()
    test.TestCovtype()
    println("END OF TEST.  WHY DOES THIS HANG AT THE END???")
  }
}
