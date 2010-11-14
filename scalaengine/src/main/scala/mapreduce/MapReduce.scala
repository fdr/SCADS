package edu.berkeley.cs.scads.mapreduce

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
  def collect(key: AvroRecord, value: AvroRecord): Unit = {
    println(key)
    println(value)
  }
  
  def reportStatus(msg: String): Unit = {
    println(msg)
  }
}



