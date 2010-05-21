package querygen

import java.lang._

class RandomUser(val numUsers: Int) {
	
	def getRandomUser():Int = {
		val res = (Math.floor(Math.random() * numUsers)).intValue()
		if (res > 0)
			res
		else
			res + 1
	}
	
}