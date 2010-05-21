package parser

import java.io._
import scala.io.Source

object ParseLowLevelLogs extends Application {
	/*
	val logFilename = System.getProperty("logFilename")
	println("logFilename=>"+System.getProperty("logFilename")+"<")
	
	val outFilename = System.getProperty("outFilename")
	println("outFilename=>"+System.getProperty("outFilename")+"<")
	var out: FileWriter = null
	out = new FileWriter( new java.io.File(outFilename), true )
	out.write("interval,opType,latency_ms\n")
	
	var intervalNum = 0

	for (line <- Source.fromFile(logFilename).getLines) {
		if (line.contains("interval")) {  // fix this
			val e = line.split(" ")
			val f = e(1).split("\n")
			intervalNum = f(0).toInt
			println("intervalNum=" + intervalNum)
		}

		if (line.contains("executed")) {
			val s = line.split(", ")

			var opType = 0
			val a = s(0).split(":")
			val b = a(4).split("\\(")
			val c = b(0).split(" ")
			val d = c(1)
			println(d)

			d match {
				case "get" => opType = 1
				case "get_set" => opType = 2
				// add on once I add more low-level ops
				case _ => 
			}

			val t = s(4).split("=")
			val u = t(1).split("\n")
			val latency = u(0)

			println(intervalNum + "," + opType + "," + latency)
			out.write(intervalNum + "," + opType + "," + latency + "\n")
			out.flush
		}
	}
	*/
	
	val logFilename = System.getProperty("logFilename")
	println("logFilename=>"+System.getProperty("logFilename")+"<")
	
	val outFilename = System.getProperty("outFilename")
	println("outFilename=>"+System.getProperty("outFilename")+"<")
	var out: FileWriter = null
	out = new FileWriter( new java.io.File(outFilename) )
	//out.write("interval,opType,latency_ms\n")
	//out.write("interval#,thread#,query#,opType,start,end,latency_ms\n")
	out.write("interval,thread,opType,start,end,latency_ms\n")
	
	val numIntervals = System.getProperty("numIntervals").toInt
	println("numIntervals=>" + System.getProperty("numIntervals") + "<")
	
	var threadNum = 0
	var intervalNum = 0
	//var queryNum = 0
	var opType = 0
	var start:String = null
	var end:String = null
	var latency = 0.0
	// each query has one get_set and 10 get's; inc the queryNum whenever a new get_set is encountered
	// first query will have queryNum=1
	// This is a HACK; will have to fix this later.
	// UPDATE: no longer used, so don't worry about it

	// skip everything until I see "starting run"
	var runStarted = false

	for (line <- Source.fromFile(logFilename).getLines) {
		if (line.contains("starting run"))
			runStarted = true
		
		if (runStarted) {
			// get intervalNum
			if (line.contains("interval")) {
				val e = line.split(" ")
				val f = e(1).split("\n")
				if (f(0).toInt < numIntervals)	
					intervalNum = f(0).toInt
				//println("intervalNum=" + intervalNum)
			}

			if (line.contains("executed")) {
				// for now, "executed" => either get or get_set
				val s = line.split(", ")

				// get threadNum
				var s1 = s(0).split("-")
				s1 = s1(1).split(" ")
				threadNum = s1(0).toInt
				//println("Thread#: " + threadNum)

				// get opType & queryNum
				val a = s(0).split(":")
				val b = a(4).split("\\(")
				val c = b(0).split(" ")
				val op = c(1)
				//println("Op: " + op)

				op match {
					case "get" => opType = 1
					case "get_set" => opType = 2
						//queryNum = queryNum + 1
					// add on once I add more low-level ops
					case _ => 
				}

				// get start
				var s2 = s(1).split("=")
				start = s2(1)

				// get end
				var s3 = s(2).split("=")
				end = s3(1)

				// get latency
				val t = s(3).split("=")
				val u = t(1).split("\n")
				//val latency = u(0)
				latency = u(0).toDouble

				//println(intervalNum + "," + opType + "," + latency)
				//println(intervalNum + "," + threadNum + "," + queryNum + "," + opType + "," + start + "," + end + "," + latency)
				//println(intervalNum + "," + threadNum + "," + opType + "," + start + "," + end + "," + latency)

				//out.write(intervalNum + "," + opType + "," + latency + "\n")
				//out.write(intervalNum + "," + threadNum + "," + queryNum + "," + opType + "," + start + "," + end + "," + latency + "\n")
				out.write(intervalNum + "," + threadNum + "," + opType + "," + start + "," + end + "," + latency + "\n")

				out.flush
			}
		}
	}
		

	
}

