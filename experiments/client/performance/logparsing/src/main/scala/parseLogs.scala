package parser

import java.io._
import scala.io.Source

object ParseLogs extends Application {
	// Get the system property by which the user indicated log directory
	// Get the system property by which the user indicated output filename (will be a .csv)
	val logDir = System.getProperty("logDir")
	println("logDir=>"+System.getProperty("logDir")+"<")

	val outFilename = System.getProperty("outFilename")
	println("outFilename=>"+System.getProperty("outFilename")+"<")
	var out: FileWriter = null
	out = new FileWriter( new java.io.File(logDir + outFilename) )
	//out.write("interval,queryType,latency_ms\n")
	out.write("interval,thread,queryType,start,end,latency_ms\n")

	var intervalNum = 0
	var threadNum:String = null
	var queryType = 0
	var start:String = null
	var end:String = null
	var latency:String = null

	val dir = new File(logDir)
	(dir.listFiles(new Filter(".log"))).foreach(file => {
		intervalNum = 0

		for (line <- Source.fromFile(file).getLines) {
			if (line.contains("switching")) {
				intervalNum += 1
				println("intervalNum=" + intervalNum)
			}

			if (line.contains("executed")) {
				/*
				val s = line.split(", ")

				var queryType = 0
				val a = s(0).split(":")
				val b = a(4).split("\\(")
				val c = b(0).split(" ")
				val d = c(1)
				println(d)

				d match {
					case "userByName" => queryType = 1
					case "userByEmail" => queryType = 2
					// add on once I add more queries
					case _ => 
				}

				val t = s(3).split("=")
				val u = t(1).split("\n")
				val latency = u(0)

				println(intervalNum + "," + queryType + "," + latency)
				out.write(intervalNum + "," + queryType + "," + latency + "\n")
				out.flush
				*/
				
				// get thread
				var entries = line.split("-")
				threadNum = (entries(1).split(" "))(0)
				
				// get queryType
				entries = line.split(", ")
				val q = (((entries(0).split(": "))(2)).split("\\("))(0)
				
				q match {
					case "userByName" => queryType = 1
					case "userByEmail" => queryType = 2
					case _ =>
				}
				
				// get start
			 	start = (entries(1).split("="))(1)
				
				// get end
				end = (entries(2).split("="))(1)
				
				// get latency
				latency = (((entries(3).split("="))(1)).split("\n"))(0)
				
				out.write(intervalNum + "," + threadNum + "," + queryType + "," + start + "," + end + "," + latency + "\n")
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

