package parser

import java.io._
import scala.io.Source

object ParsePerThreadLogs extends Application {
	// Codes for op level
	val PRIMITIVE=1
	val OP=2
	val QUERY=3
	
	val logDir = System.getProperty("logDir")
	println("Logs obtained from " + logDir + "...")
	// logDir shouldn't end with a "/"
	
	// Read in all log files from directory indicated, one at a time.
	var out: FileWriter = null
	
	val dir = new File(logDir)
	(dir.listFiles(new Filter(".log"))).foreach(file => {
		println("Parsing " + file.toString + "...")
		val fnameArray = file.toString.split("/")
		val fname = fnameArray(fnameArray.length-1)
		
		val prefix = fname.split("\\.")(0)
		out = new FileWriter( new java.io.File(logDir + "/" + prefix + ".csv") )
		out.write("threadNum, queryNum, opLevel, opType, start_ms, end_ms, latency_ms\n")
		
		val threadNum = prefix.split("-")(1)
		var queryNum = 0
		
		// Read in file, one line at a time.
		var opLevel = 0
		var opType = 0
		for (line <- Source.fromFile(file).getLines) {
			if (line.contains("starting"))
				queryNum = queryNum+1
			
			if (line.contains("executed")) {
				val opText = line.split("executed: ")(1).split("\\(")(0)

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
					case "thoughtsByHashTag" => opLevel=Query; opType=4
				}
				
				val start = line.split(", ")(1).split("=")(1)
				val end = line.split(", ")(2).split("=")(1)
				val latency = line.split(", ")(3).split("=")(1)
				
				out.write(threadNum + ", " + queryNum + ", " + opLevel + ", " + opType + ", " + start + ", " + end + ", " + latency)
				out.flush
			}
		}
	})
	
}

class Filter(pattern:String) extends FilenameFilter {
  	def accept (dir:File, name:String):boolean = {
    	return name.toLowerCase().endsWith(pattern.toLowerCase());
  	}
}
