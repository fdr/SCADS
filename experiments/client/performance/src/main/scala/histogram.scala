import java.util.Random
import java.io._
import scala.io.Source

// TODO:  redo so it takes a map from int (breaks) to int (counts)
// Move the "readFromFile" fcn to another class -- something like, "RFileParser" -- that's also in this file

class Histogram (histFilename: String) {
	var breaks:List[Int] = Nil
	var counts:List[Int] = Nil
	

/*	def readFromFile() = {
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
	}*/
	
	def sample():Int = {
		0 // placeholder
	}
}

