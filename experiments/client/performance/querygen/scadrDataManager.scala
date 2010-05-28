package edu.berkeley.cs.scads.querygen

//import edu.berkeley.cs.scads.model._
//import edu.berkeley.cs.scads.thrift._

import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._
import piql._

import java.io._
import java.net._

//import querygen._
//import edu.berkeley.cs.scads.model.Environment

class SCADrDataManager {
	def populateDB(paramMap:Map[String, List[String]], emailDupFactor:Int, thoughtsPerUser:Int, subsPerUser:Int)(implicit env:Environment) = {
		println("Checking whether db is populated...")
		val nodes = env.placement.locate("ent_user", "user1")	// This assumes that db pop => I will have created the "ent_user" ns, and "user1" will have been added
		val rand = new scala.util.Random()
		val node = nodes(rand.nextInt(nodes.length))

		val sz = node.useConnection((c) => {
			c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
		})

		if (sz == 0) {
			println("DB is NOT populated yet.")
			println("Adding users & their thoughts...")

			val numUsers = paramMap("userByName").length
			val numEmails = paramMap("userByEmail").length
			val thoughtstreamLength = paramMap("thoughtstream")(0)
			
			val gen = new SimpleThoughtGenerator(thoughtsPerUser, env)
			(1 to numUsers).foreach((i) => {
				// Create user
				val u = new user
				u.name(paramMap("userByName")(i-1))
				u.password("secret")
				u.email(paramMap("userByEmail")(i % (numUsers/emailDupFactor)))
				u.save

				gen.generateThoughts(paramMap("userByName")(i-1))
			})
			println("Users & thoughts added.")
			
			println("Adding subscriptions...")

			val subGen = new SimpleSubscriptionGenerator(paramMap("userByName"), subsPerUser, env)
			(1 to numUsers).foreach((i) => {
				subGen.generateSubscriptions(paramMap("userByName")(i-1))
			})

			println("Finished adding subscriptions.")
			
		}
		println("DB is populated now.")	
		
	}
	
	
	def populateDB(paramMap:Map[String, List[String]], emailDupFactor:Int, thoughtsPerUser:Int, 
		subsPerUser:Int, hashTagsPerThought:Int)(implicit env:Environment) = {
		println("Checking whether db is populated...")
		val nodes = env.placement.locate("ent_user", "user1")	// This assumes that db pop => I will have created the "ent_user" ns, and "user1" will have been added
		val rand = new scala.util.Random()
		val node = nodes(rand.nextInt(nodes.length))

		val sz = node.useConnection((c) => {
			c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
		})

		if (sz == 0) {
			println("DB is NOT populated yet.")
			println("Adding users & their thoughts (with hash tags)...")

			val numUsers = paramMap("userByName").length
			val numEmails = paramMap("userByEmail").length
			val thoughtstreamLength = paramMap("thoughtstream")(0)
			
			val hashTagGenerator = new SimpleHashTagGenerator(hashTagsPerThought, paramMap("thoughtsByHashTag"), env)
			//val gen = new SimpleThoughtGenerator(thoughtsPerUser, env)
			val thoughtGenerator = new SimpleThoughtGeneratorWithHashTags(thoughtsPerUser, hashTagGenerator, env)
			
			(1 to numUsers).foreach((i) => {
				// Create user
				val u = new user
				u.name(paramMap("userByName")(i-1))
				u.password("secret")
				u.email(paramMap("userByEmail")(i % (numUsers/emailDupFactor)))
				u.save

				//gen.generateThoughts(paramMap("userByName")(i-1))
				thoughtGenerator.generateThoughts(paramMap("userByName")(i-1))
				
				if ((i % 100) == 0)
					println("Added " + i + " users...")
			})
			println("Users & thoughts added.")
			
			println("Adding subscriptions...")

			val subGen = new SimpleSubscriptionGenerator(paramMap("userByName"), subsPerUser, env)
			(1 to numUsers).foreach((i) => {
				subGen.generateSubscriptions(paramMap("userByName")(i-1))
			})

			println("Finished adding subscriptions.")
			
		}
		println("DB is populated now.")	
		
	}
	
	
	
	
	def getParamMap(numUsers:Int, emailDupFactor:Int, numThoughts:Int): Map[String, List[String]] = {
		// Usernames
		var usernames:List[String] = Nil
		(1 to numUsers).foreach((i) => {
			usernames = ("user" + i).toString :: usernames
		})
		usernames = usernames.reverse
		
		// Emails
		// emailDupFactor = how many users share each email addr
		// Useful for "userByEmail" query
		var emails:List[String] = Nil
		(1 to (numUsers/emailDupFactor)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse
		
		val paramMap = Map("userByName"->usernames, "userByEmail"->emails, "thoughtstream"->List(numThoughts.toString))	// "thoughtstream" also reuses "usernames"
		paramMap
	}
	
	
	def getParamMap(numUsers:Int, emailDupFactor:Int, numThoughts:Int, 
		numDistinctHashTags:Int): Map[String, List[String]] = {
		// Usernames
		var usernames:List[String] = Nil
		(1 to numUsers).foreach((i) => {
			usernames = ("user" + i).toString :: usernames
		})
		usernames = usernames.reverse
		
		// Emails
		// emailDupFactor = how many users share each email addr
		// Useful for "userByEmail" query
		var emails:List[String] = Nil
		(1 to (numUsers/emailDupFactor)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse
		
		// Hash Tags
		var hashTags:List[String] = Nil
		(1 to numDistinctHashTags).foreach((i) => {
			hashTags = ("tag" + i).toString :: hashTags
		})
		hashTags = hashTags.reverse
		
		val paramMap = Map("userByName"->usernames, "userByEmail"->emails, "thoughtstream"->List(numThoughts.toString),
			"thoughtsByHashTag"->hashTags)	
			// "thoughtstream" reuses "usernames"; "thoughtsByHashTag" reuses "numThoughts", since this is related to how many can 
			// be displayed.  Could update in future.
		paramMap
	}
	
	
	// Just usernames, emails, hashTags; no limits (eg, thoughtstreamLength)
	def getDataParamMap(numUsers:Int, emailDupFactor:Int, numDistinctHashTags:Int): Map[String, List[String]] = {
		// Usernames
		var usernames:List[String] = Nil
		(1 to numUsers).foreach((i) => {
			usernames = ("user" + i).toString :: usernames
		})
		usernames = usernames.reverse
		
		// Emails
		// emailDupFactor = how many users share each email addr
		// Useful for "userByEmail" query
		var emails:List[String] = Nil
		(1 to (numUsers/emailDupFactor)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse
		
		// Hash Tags
		var hashTags:List[String] = Nil
		(1 to numDistinctHashTags).foreach((i) => {
			hashTags = ("tag" + i).toString :: hashTags
		})
		hashTags = hashTags.reverse
		
		val dataParamMap = Map("userByName"->usernames, "userByEmail"->emails, "thoughtsByHashTag"->hashTags)	
		dataParamMap
	}
	
	
	def serializeParamMap(paramMap:Map[String, List[String]], filename:String) {
		val out = new ObjectOutputStream( new FileOutputStream(filename) )
        out.writeObject(paramMap);
        out.close();
	}
	
	
	def deserializeParamMap(filename:String):Map[String, List[String]] = {
        val fin = new ObjectInputStream( new FileInputStream(filename) )
        val paramMap = fin.readObject().asInstanceOf[Map[String, List[String]]]
        fin.close
        paramMap
	}
	
} 