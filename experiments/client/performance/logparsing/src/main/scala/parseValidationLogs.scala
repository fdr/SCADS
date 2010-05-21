package parser

import java.io._
import scala.io.Source

object ParseValidationLogs extends Application {
	// Codes for op level
	val PRIMITIVE=1
	val OP=2
	val QUERY=3
	
	val opTextSet = Set("get", "get_set", "singleGet", "prefixGet", "sequentialDereferenceIndex", "prefixJoin", "pointerJoin", "materialize",
		"selection", "sort", "topK", "userByName", "userByEmail", "thoughtstream", "thoughtsByHashTag")
	
	val logDir = System.getProperty("logDir")
	println("Logs obtained from " + logDir + "...")		// logDir shouldn't end with a "/"
	val outputDir = System.getProperty("outputDir")
	val outputFilename = System.getProperty("outputFilename")
	if (outputDir == null)
		println("Logs output to " + logDir + "/" + outputFilename + "...")
	else
		println("Logs output to " + outputDir + "/" + outputFilename + "...")
	
	
	// Read in all log files from directory indicated, one at a time.
	val out = new FileWriter( new java.io.File(outputDir + "/" + outputFilename) ) // fix this!  if outputDir= null, use logDir
	out.write("threadNum, opLevel, opType, start_ms, end_ms, latency_ms\n")
	
	val dir = new File(logDir)
	
	var numLines = 0
	
	// Consider files in descending order b/c of the way they're produced by the rolling file appender (? see what happens when there are > 2 files)
	(dir.listFiles(new LogFilter(".log")).toList.reverse).foreach(file => {
		for (line <- Source.fromFile(file).getLines) {
			if (line.contains("Thread")) {
				var entries = line.split(" ")
				val opText = entries(6).split("\\(")(0)
				if (opTextSet.contains(opText)) {
					val threadNum = entries(5).split("-")(1).split(":")(0)

					var opLevel = 0
					var opType = 0
					
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

						case "userByName" => opLevel=QUERY; opType=1
						case "userByEmail" => opLevel=QUERY; opType=2
						case "thoughtstream" => opLevel=QUERY; opType=3
						case "thoughtsByHashTag" => opLevel=QUERY; opType=4
					}
					
					entries = line.split(", ")
					val start = entries(1).split("=")(1)
					val end = entries(2).split("=")(1)
					val latency = entries(3).split("=")(1)
					
					out.write(threadNum + ", " + opLevel + ", " + opType + ", " + start + ", " + end + ", " + latency)
					out.flush
				}
				
				numLines += 1
				if (numLines % 1000 == 0)
					println("Parsed " + numLines + " lines...")
			}
		}
	})	
}