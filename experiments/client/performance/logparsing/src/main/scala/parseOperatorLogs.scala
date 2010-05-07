package parser

import java.io._
import scala.io.Source

// assumes various "chunks" indicated by #A, #B, size(A), size(B)

object ParseOperatorLogs extends Application {
	// Codes for op level
	val PRIMITIVE=1
	val OP=2
	
	val logDir = System.getProperty("logDir")
	println("Logs obtained from " + logDir + "...")		// logDir shouldn't end with a "/"
	val outputFilename = System.getProperty("outputFilename")
	println("Logs output to " + outputFilename)
	
	// Read in all log files from directory indicated, one at a time.
	val out = new FileWriter( new java.io.File(outputFilename) )
	out.write("threadNum, opLevel, opType, aSize, bSize, numA, numB, start_ms, end_ms, latency_ms\n")
	
	var aSize = 0
	var bSize = 0
	var numA = 0
	var numB = 0
	var warmupDone = false
	
	val dir = new File(logDir)
	
	// Consider files in descending order b/c of the way they're produced by the rolling file appender (? see what happens when there are > 2 files)
	(dir.listFiles(new LogFilter(".log")).toList.reverse).foreach(file => {
		println(file.toString)
		
		var threadNum = 0
		var opType = 0
		var opLevel = 0
		var start:String = null
		var end:String = null
		var latency:String = null
		
		var lineNum = 0

		for (line <- Source.fromFile(file).getLines) {
			if (line.contains("New data size pair")) {
				val entries = line.split(" +")
				aSize = entries(12).substring(0,entries(12).length-1).toInt
				bSize = entries(15).substring(0,entries(15).length-1).toInt
			}
			
			if (line.contains("Starting run with")) {
				warmupDone = true
				
				val entries = line.split(" +")
				numA = entries(9).split("=")(1).split(",")(0).toInt
				numB = entries(10).split("=")(1).split("\\.")(0).toInt
			}
			
			if (line.contains("Thread-") && !line.contains("is thinking")) {
				val entries = line.split(", ")
				
				threadNum = entries(0).split(" +")(5).split("-")(1).split(":")(0).toInt
				
				val opText = entries(0).split(" +")(6).split("\\(")(0)
				opText match {
					case "get" => opLevel=PRIMITIVE; opType=1
					case "get_set" => opLevel=PRIMITIVE; opType=2
					
					case "singleGet" => opLevel=OP; opType=1
					case "prefixGet" => opLevel=OP; opType=2
					case "sequentialDereferenceIndex" => opLevel=OP; opType=3
					case "prefixJoin" => opLevel=OP; opType=4
					case "pointerJoin" => opLevel=OP; opType=5
					case "materialize" => opLevel=OP; opType=6
					case "selection" => opLevel=OP; opType=7
					case "sort" => opLevel=OP; opType=8
					case "topK" => opLevel=OP; opType=9
				}
				
				start = entries(1).split("=")(1)
				
				end = entries(2).split("=")(1)
				
				latency = entries(3).split("=")(1).substring(0,entries(3).split("=")(1).length)
				
				if (warmupDone) {
					//out.write("threadNum, opLevel, opType, aSize, bSize, numA, numB, start_ms, end_ms, latency_ms\n")
					out.write(threadNum + ", " + opLevel + ", " + opType + ", " + aSize + ", " + bSize + ", " + numA + ", " 
					+ numB + ", " + start + ", " + end + ", " + latency)
					out.flush
				}
			}
			
			if ((lineNum % 100000) == 0)
				println("Parsed " + lineNum + " lines...")
				
			lineNum = lineNum + 1
		}
	})
	
}

class LogFilter(pattern:String) extends FilenameFilter {
  	def accept (dir:File, name:String):boolean = {
    	return name.toLowerCase().contains(pattern.toLowerCase());
  	}
}



