package edu.berkeley.cs.scads.querygen

//import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._
import piql._


abstract class HashTagGenerator {
	def getHashTag: String

	def assignHashTags(referringThought: thought)
}

class SimpleHashTagGenerator (
	val numHashTags: Int, 
	val hashTags: List[String], 
	implicit val env:Environment
) extends HashTagGenerator {
	val rand = new scala.util.Random()

	// Uniform distribution over hash tags
	def getHashTag: String = {
		val hashTagIdx = rand.nextInt(hashTags.length)
		hashTags(hashTagIdx)
	}

	// Each thought gets the same # of tags
	def assignHashTags(referringThought: thought) = {
		
		var j = 0
		var tags:List[String] = Nil
		var first = true
		
		while (j < numHashTags) {
			// Generate hashTag
			val tag = getHashTag
			
			if (first || !tags.exists(n => n == tag)) {
				first = false
				tags = tag :: tags
				val h = new hashTag
				h.name(tag)
				h.referringThought(referringThought)
				h.save
				
				j = j+1
			}
		}
	}
}

