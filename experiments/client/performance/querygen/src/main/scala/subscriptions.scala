package edu.berkeley.cs.scads.querygen

//import edu.berkeley.cs.scads.model.Environment
//import querygen.RandomUser
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._
import piql._


abstract class SubscriptionGenerator {
	def generateSubscriptions(username: String)
}

class SimpleSubscriptionGenerator (val usernames:List[String], val numSubscriptions:Int, implicit val env:Environment) extends SubscriptionGenerator {
	val rdm = new RandomUser(usernames.length)

	def generateSubscriptions(username:String) = {
		var subs:List[Int] = Nil
		var j = 0
		var first = true

		while (j < numSubscriptions) {
			// Generate # => user to follow
			val sub = new Subscription
			sub.approved = true			// might want to play with this in future
			sub.owner = Queries.userByName(username).apply(0)
			val usernum = rdm.getRandomUser

			// Make sure we haven't already added this subscription
			if (first || !subs.exists(n => n == usernum)) {
				first = false
				subs = usernum :: subs
				sub.target = Queries.userByName(usernames(usernum)).apply(0)
				sub.save
				j = j+1
			}
		}
	}
}