package edu.berkeley.cs.scads.util
package twitter

import scala.io.Source

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

object TwitterLoader {
  def fromGZIPfile(filename:String):Source = {
    Source.fromInputStream(new GZIPInputStream(new FileInputStream(filename)))
  }
  def loadFile(filename: String, ns: SpecificNamespace[HashLongRec, StringRec]) = {
    var total_tweets = 0
    var batch = List[(HashLongRec, StringRec)]()
    var batchLength = 0
    var minVal = Long.MaxValue
    var maxVal:Long = 0
    val inputSource = if (filename.endsWith(".gz")) {
      fromGZIPfile(filename)
    } else {
      Source.fromFile(filename)
    }
    for (line <- inputSource.getLines) {
      val tweet = JsonParser.parseJson(line)
      if (tweet.contains("id")) {
        val longVal = tweet("id").asInstanceOf[BigInt].longValue
        if (longVal < minVal)
          minVal = longVal
        if (longVal > maxVal)
          maxVal = longVal
        batch = Tuple2(HashLongRec(Hash.hashMurmur2(longVal.toString.toArray.map(_.toByte)),
                                   longVal), StringRec(line)) :: batch
        batchLength += 1
        if (batchLength >= 1000) {
          // write the batch out
          ns ++= batch
          total_tweets = total_tweets + batchLength
          batch = List[(HashLongRec, StringRec)]()
          batchLength = 0
          println(filename + ": inserted " + total_tweets + " tweets")
        }
      }
    }
    if (batchLength > 0) {
      ns ++= batch
      total_tweets = total_tweets + batchLength
      println(filename + ": inserted " + total_tweets + " tweets")
    }
    println("min: " + minVal + " , max: " + maxVal)
    (total_tweets, minVal, maxVal)
  }
}
