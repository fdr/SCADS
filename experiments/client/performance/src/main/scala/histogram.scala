import java.util.Random
import java.io._
import scala.io.Source

class Histogram (histFilename: String) {
	var breaks:List[Int] = Nil
	var counts:List[Int] = Nil
	
	def readFromFile() = {
		//breaks = new List
		//counts = new List
		var first = true
		for (line <- Source.fromFile(histFilename).getLines) {
			// Skip first line
			if (first) {
				first = false
			} else {
				val entries = line.split(",")
				breaks = entries(1).toInt :: breaks
				
				val entries2 = entries(2).split("\n")
				counts = entries2(0).toInt :: counts
			}
		}
		
		breaks = breaks.reverse
		counts = counts.reverse
	}
	
	def sample():Int = {
		0 // placeholder
	}
}

