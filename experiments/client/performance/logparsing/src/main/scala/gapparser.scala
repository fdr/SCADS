import java.io._
import scala.io.Source

object GapParser extends Application {
	val inFilename = System.getProperty("inFilename")
	println("inFilename=>"+System.getProperty("inFilename")+"<")
	
	val outFilename = System.getProperty("outFilename")
	println("outFilename=>"+System.getProperty("outFilename")+"<")
	var out: FileWriter = null
	out = new FileWriter( new java.io.File(outFilename) )
	out.write("type,start,end\n")
	
	
	for (line <- Source.fromFile(inFilename).getLines) {
		val entries = line.split(",")
		val entries2 = entries(0).split(": ")
		
		val op = entries2(2)
		var opType = -1
		var start = ""
		var end = ""

		if (op.contains("get(")) {
			opType = 0
			
			val entries3 = entries(2).split("=")
			start = entries3(1)
			
			val entries4 = entries(3).split("=")
			end = entries4(1)
		} else if (op.contains("get_set")) {
			opType = 1
			
			val entries3 = entries(1).split("=")
			start = entries3(1)
			
			val entries4 = entries(2).split("=")
			end = entries4(1)
		} else if (op.contains("userByEmail")) {
			opType = 2
			
			val entries3 = entries(1).split("=")
			start = entries3(1)
			
			val entries4 = entries(2).split("=")
			end = entries4(1)
		} else {
			println(3)
		}

		out.write(opType + "," + start + "," + end + "\n")
		out.flush

	}
	
	
}