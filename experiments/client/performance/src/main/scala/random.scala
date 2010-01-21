package querygen

import java.lang._

class RandomUser(val numUsers: Int) {
	
	def getRandomUser():Int = {
		return (Math.floor(Math.random() * numUsers)).intValue()
	}
	
}