package edu.berkeley.cs.scads.util

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

import edu.berkeley.cs.scads.mapreduce.{Mapper, Context}

class TestMapper extends Mapper {
  def map(key: AvroRecord, value: AvroRecord, context: Context): Unit = {
    val keyInt = key.asInstanceOf[IntRec].f1
    val valueStr = value.asInstanceOf[StringRec].f1
    context.collect(key, StringRec(valueStr + "-" + keyInt))
  }
}

class TestScadsMapper {
  def run() {
    // Creates 3 local storage server
    val storageHandler = TestScalaEngine.getTestHandler(2)

    // Creates a local client
    val client = new ScadsCluster(storageHandler.head.root)
    val storageServer = client.getAvailableServers

    // Creates a partition and replicates it across all three storage servers
    val ns1 = client.createNamespace[IntRec, StringRec](
        "getputtest",
        List((None, List(storageServer(0))),
             (IntRec(50), List(storageServer(1)))))

    // Writes data
    (1 to 100).foreach(index => ns1.put(IntRec(index),
                                        StringRec("VAL" + index)))

    // Run Mapper
    ns1.executeMappers(IntRec(40), IntRec(60), classOf[TestMapper])
  }
}

object TestScadsMapper {
  def main(args: Array[String]) {
    val test = new TestScadsMapper()
    test.run()
    println("END OF TEST.  WHY DOES THIS HANG AT THE END???")
  }
}
