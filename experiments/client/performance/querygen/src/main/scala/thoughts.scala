package edu.berkeley.cs.scads.querygen

//import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._
import piql._


abstract class ThoughtGenerator {
	def generateThoughts(username: String)
}

class SimpleThoughtGenerator (val numThoughts:Int, implicit val env:Environment) extends ThoughtGenerator {
	def generateThoughts(username:String) = {
		var timestamp = 0

		(1 to numThoughts).foreach((i) => {
			val th = new Thought
			th.owner = Queries.userByName(username).apply(0)
			th.text = "thought"
			//th.timestamp(System.currentTimeMillis().toInt)  // TODO:  this doesn't give the right time.  Replace with integers that count up.
			th.timestamp = timestamp
			th.save
			
			timestamp += 1
		})
	}
}

/*
class SimpleThoughtGeneratorWithHashTags (
	val numThoughts:Int, 
	val hashTagGenerator:HashTagGenerator, 
	implicit val env:Environment
) extends ThoughtGenerator {
	def generateThoughts(username:String) = {
		var timestamp = 0
		
		(1 to numThoughts).foreach((i) => {
			val th = new Thought
			th.owner = Queries.userByName(username).apply(0)
			th.text = "thought"
			//th.timestamp(System.currentTimeMillis().toInt)  // TODO:  this doesn't give the right time.  Replace with integers that count up.
			th.timestamp = timestamp
			th.save
			
			hashTagGenerator.assignHashTags(th)
			
			timestamp += 1
		})
	}
}
*/

/*
class VariableNumThoughtsThoughtGenerator (val histogram:Histogram, implicit val env:Environment) extends ThoughtGenerator {
	def generateThoughts(username:String) = {
		(1 to getNumThoughts).foreach((i) => {
			val th = new thought
			th.owner(username)
			th.thought("thought")
			th.timestamp(System.nanoTime().toInt)  // this time doesn't seem to work well.  could ask MA what he uses
			th.save
		})
	}
	
	def getNumThoughts:Int = {
		histogram.sample
		// TODO:  finish sample fcn
	}
}
*/