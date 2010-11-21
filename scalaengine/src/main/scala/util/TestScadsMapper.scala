package edu.berkeley.cs.scads.util

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

import edu.berkeley.cs.scads.mapreduce.{Mapper, MapperContext}

class TestMapper extends Mapper {
  def map(key: AvroRecord, value: AvroRecord, context: MapperContext): Unit = {
    val keyInt = key.asInstanceOf[IntRec].f1
    val valueStr = value.asInstanceOf[StringRec].f1

    val lookup = context.getKey[IntRec, StringRec]("lookuptest", key.asInstanceOf[IntRec]) match {
      case None => "NULL"
      case Some(x) => x
    }

    //context.collect(key, StringRec(valueStr + "-" + keyInt))
    context.collect(IntRec(keyInt%2), StringRec(valueStr + "-" + keyInt + lookup))
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

    // Fill the test lookup table.
    val ns2 = client.createNamespace[IntRec, StringRec](
        "lookuptest",
        List((None, List(storageServer(0))),
             (IntRec(50), List(storageServer(1)))))
    (1 to 100).foreach(index => ns2.put(IntRec(index),
                                        StringRec("LOOKUP" + (index % 3 + 17))))

    val ns3 = client.getNamespace[IntRec, StringRec]("getputtest")
    // Run Mapper
    ns3.executeMappers(IntRec(40), IntRec(60), classOf[TestMapper])
  }
}

object TestScadsMapper {
  def main(args: Array[String]) {
    val test = new TestScadsMapper()
    test.run()
    println("END OF TEST.  WHY DOES THIS HANG AT THE END???")
  }
}
