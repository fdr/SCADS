package edu.berkeley.cs.scads.mapreduce

import scala.collection.mutable.{Buffer, ListBuffer, HashMap, Map}
import edu.berkeley.cs.avro.marker.AvroRecord


trait Context {
  def reportStatus(msg: String): Unit
  def collect(key: AvroRecord, value: AvroRecord): Unit
}


trait Mapper {
  def map(key: AvroRecord, value: AvroRecord, context: Context): Unit
}


trait Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord], context: Context): Unit
}


/**
 * A quick implementation of the MapperContext class.
 */
class MapperContext extends Context {
  
  val mapperOutput = HashMap.empty[ AvroRecord, Buffer[AvroRecord] ]
  
  def collect(key: AvroRecord, value: AvroRecord): Unit = {
    mapperOutput.getOrElseUpdate(key, ListBuffer[AvroRecord]()).append(value)
  }
  
  def reportStatus(msg: String): Unit = {
    println(msg)
  }
}



