import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
import edu.berkeley.cs.scads.model.TestCluster
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._

import querygen.RandomUser

//import java.util._

//import java.lang._

//println("hi")

implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor

// Testing thought generation w/ hash tags

val numHashTags=5	// per thought; max = 10

val numDistinctHashTags = 10
var hashTags:List[String] = Nil

(1 to numDistinctHashTags).foreach((i) => {
	hashTags = ("tag" + i).toString :: hashTags
})
hashTags = hashTags.reverse

val hashTagGenerator = new SimpleHashTagGenerator(numHashTags, hashTags, env)

val numThoughtsPerUser = 20
val thoughtGenerator = new SimpleThoughtGeneratorWithHashTags(numThoughtsPerUser, hashTagGenerator, env)

val numUsers = 100
(1 to numUsers).foreach((i) => {
	// Create user
	val u = new user
	u.name("user" + i)
	u.password("secret")
	u.email("user" + i + "@test.com")
	u.save

	thoughtGenerator.generateThoughts("user" + i)
})

println(Queries.thoughtsByHashTag("tag1",10))

/*
val u1 = new user
u1.name("marmbrus")
u1.password("pass")
u1.email("marmbrus@berkeley.edu")
u1.save

val u2 = new user
u2.name("kristal")
u2.password("pass")
u2.email("kristal@berkeley.edu")
u2.save


val res = Queries.userByName("marmbrus").apply(0)
println()
println("ANS:")
println(res)
println()

val res2 = Queries.userByName("kristal").apply(0).name
println()
println("ANS:")
println(res2)
println()



val sub = new subscription
sub.approved(true)
sub.owner("marmbrus")
sub.target("kristal")
sub.save

val time = System.nanoTime().toInt
val th = new thought
th.owner("kristal")
th.thought("i love research")
th.timestamp(time)
th.save

val h = new hashTag
h.name("cal")
h.referringThought(th)
h.save

println(u2.myThoughts(1))

println(this)
*/

/*
println("Checking whether db is populated...")
val nodes2 = env.placement.locate("ent_user", "user1")	// This assumes that db pop => I will have created the "ent_user" ns, and "user1" will have been added
val rand2 = new scala.util.Random()
val node2 = nodes2(rand2.nextInt(nodes2.length))



val sz2 = node2.useConnection((c) => {
	c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
})

println("Result = " + sz2)  // if > 0, db is populated

val numUsers=10
val numSubs = 2

if (sz2 == 0) {
	println("DB is NOT populated yet.")

	println("Adding users & their thoughts...")
	// Create data so I can test "thoughtstream" query
	(1 to numUsers).foreach((i) => {
		// Create user
		val u = new user
		u.name("user" + i)
		u.password("secret")
		u.email("user" + i + "@test.com")
		u.save

		val sTG = new SimpleThoughtGenerator(20, env)
		sTG.generateThoughts("user" + i)
	})

	println("Checking whether db is populated...")
	val nodes = env.placement.locate("namespace", "key")
	val rand = new scala.util.Random()
	val node = nodes(rand.nextInt(nodes.length))

	val sz = node.useConnection((c) => {
		c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
	})

	println("Result = " + sz)  // if > 0, db is populated
}
println("DB is populated now.")



// Add subscriptions (do after creating all users)
println("Adding subscriptions...")
var usernames:List[String] = Nil
(1 to numUsers).foreach((i) => {
	usernames = ("user" + i).toString :: usernames
})
val gen = new SimpleSubscriptionGenerator(usernames, numSubs, env)
(1 to numUsers).foreach((i) => {
	gen.generateSubscriptions("user" + i)
})
println("Finished adding subscriptions.")

println(Queries.userByName("user6").apply(0).thoughtstream(30))
*/

/*
println("Adding subscriptions...")
val rdm = new RandomUser(numUsers)

(1 to numUsers).foreach((i) => {
	var subs:List[Int] = Nil
	
	var j = 0
	//(1 to 10).foreach((j) => {
	
	var first = true
		
	while (j < 10) {
		val sub = new subscription
		sub.approved(true)
		sub.owner("user" + i)
		val usernum = rdm.getRandomUser

		if (first || !subs.exists(n => n == usernum)) {
			first = false
			subs = usernum :: subs
			sub.target("user" + usernum)
			sub.save
			j = j+1
		}
	//})
	}
})

println("Finished adding subscriptions...")

//println(Queries.userByName("user6").apply(0).thoughtstream(30))
val q = new SCADrQueryThoughtstream("user6", 10, env)
val start = System.nanoTime()
//println(q.execute)
val res = q.execute
val end = System.nanoTime()
val latency = end-start
println("start=" + start + ", end=" + end + ", latency=" + (latency/1000000.0) + "ms")
println(res)
println(q.toString)
println(q.queryType)
*/

/*
println("Old way:")
val s = System.nanoTime()
val r = Queries.userByName("user6").apply(0).thoughtstream(10)
val e = System.nanoTime()
val l = e - s
println("start=" + s + ", end=" + e + ", latency=" + (l/1000000.0) + "ms")
println(r)
*/

/*
object ClusterTester extends Application {
	println("enter main")
	
	// Configure storage engine
	//Queries.configureStorageEngine(new StorageNode("r21", 9000))  // set responsibility policy -- r21 instead of ip

	// Testing ZooKeptCluster
	implicit val env = new Environment
	env.placement = new ZooKeptCluster("r13:3000")
	env.session = new TrivialSession
	env.executor = new TrivialExecutor

	val u1 = new user
	u1.name("marmbrus")
	u1.password("pass")
	u1.email("marmbrus@berkeley.edu")
	u1.save

	val res = Queries.userByName("marmbrus").apply(0).name
	println()
	println("ANS:")
	println(res)
	println()
}
*/



/*
val u1 = new user
u1.name("marmbrus")
u1.password("pass")
u1.email("marmbrus@berkeley.edu")
u1.save

val res = Queries.userByName("marmbrus").apply(0).name
println()
println("ANS:")
println(res)
println()
*/

// Populate the db
/*
(1 to 1000).foreach((i) => {
	val u = new user
	u.name("user" + i)
	u.password("secret")
	u.email("user" + i + "@test.com")
	u.save
})
*/


/*
println("User 72: " + Queries.userByName("user72").apply(0).name)
println("User 36: " + Queries.userByName("user36").apply(0).email)

var idx = (Math.floor(Math.random() * 1000)).intValue()
//println(idx)

println("User " + idx + ": " + Queries.userByName("user" + idx).apply(0).name)
*/

// Testing SCADrQueryUserByName
/*
val rdm = new RandomUser
rdm.setNumUsers(1000)
val idx = rdm.getRandomUser
println("User " + idx + ": " + Queries.userByName("user" + idx).apply(0).name)
val q = new SCADrQueryUserByName("user" + idx, env)
println("Query: "+ q.toString)
*/

// Testing SCADrQueryUserByEmail
/*
case class SCADrQueryUserByEmail (val email: String, implicit val env: Environment) extends SCADrQuery() {
	def queryType: String = "userByEmail"
	
	def execute = {
		val res = Queries.userByEmail(email)
		res
	}
}
*/
/*
(1 to 1000).foreach((i) => {
	val u = new user
	u.name("user" + i)
	u.password("secret")
	u.email("user@test.com")
	u.save
})

val q = new SCADrQueryUserByEmail("user@test.com", env)
println("Query: " + q.toString)

val startt = System.nanoTime()
q.execute
val endt = System.nanoTime()
val latency = endt-startt
println("Latency = " + (latency/1000000.0))
*/

// Testing UniformParamGenerator
/*
// Make list of possible params
var possibleParams = List("user1")
(2 to 1000).foreach((i) => {
	possibleParams = ("user" + i) :: possibleParams
})
possibleParams = possibleParams.reverse
println(possibleParams.toString)

val u = UniformParamGenerator(possibleParams)
val q = new SCADrQueryUserByName(u.generateParam, env)
println("Query: " + q.toString)
*/

// Testing UniformParamGenerator for userByEmail
/*
// Generate db, where each email is shared by 20 users (1000 users => 50 email addresses)
val numUsers = 1000
val emailDup = 20

var emails:List[String] = Nil
(1 to (numUsers/emailDup)).foreach((i) => {
	emails = ("user" + i + "@berkeley.edu") :: emails
})
emails = emails.reverse

//(1 to numUsers).foreach((i) => {
(1 to 100).foreach((i) => {	
	val u = new user
	u.name("user" + i)
	u.password("secret")
	//u.email("user@test.com")
	u.email(emails(i % (numUsers/emailDup)))
	u.save
})

val u = UniformParamGenerator(emails)

(1 to 10).foreach((i) => {
	val q = new SCADrQueryUserByEmail(u.generateParam, env)
	println("Query: " + q.toString)
})
*/


// Testing MixVector
/*
val mix100 = new MixVector(Map("userByName"->1.0,"b"->0.0,"c"->0.0))
println(mix100.toString)
println(mix100.sampleQueryType)
println(mix100.sampleQueryType)
println(mix100.sampleQueryType)
*/

// Testing MixVector for userByEmail
/*
val mix100b = new MixVector(Map("userByEmail"->1.0,"b"->0.0))
println(mix100b.toString)
println(mix100b.sampleQueryType)
println(mix100b.sampleQueryType)
println(mix100b.sampleQueryType)
*/


// Testing SimpleSCADrQueryGenerator
/*
val mix100 = new MixVector(Map("userByName"->1.0,"b"->0.0,"c"->0.0))

var possibleParams = List("user1")
(2 to 10).foreach((i) => {
	possibleParams = ("user" + i) :: possibleParams
})
possibleParams = possibleParams.reverse

val paramMap = Map("userByName"->possibleParams, "b"->List("b"), "c"->List("c"))
println(paramMap.toString)
val qg = new SimpleSCADrQueryGenerator(mix100, paramMap, env)
val q = qg.generateQuery
q.execute
println(q.toString)

(1 to 10).foreach((i) => println(qg.generateQuery.toString))
*/

// Testing SimpleSCADrQueryGenerator for userByEmail
/*
val mix100b = new MixVector(Map("userByEmail"->1.0,"userByName"->0.0))

val numUsers = 1000
val emailDup = 20

var emails:List[String] = Nil
(1 to (numUsers/emailDup)).foreach((i) => {
	emails = ("user" + i + "@berkeley.edu") :: emails
})
emails = emails.reverse

(1 to numUsers).foreach((i) => {
//(1 to 100).foreach((i) => {	
	val u = new user
	u.name("user" + i)
	u.password("secret")
	//u.email("user@test.com")
	u.email(emails(i % (numUsers/emailDup)))
	u.save
})

val paramMap = Map("userByEmail"->emails, "userByName"->List("b"))
println(paramMap.toString)

val qg = new SimpleSCADrQueryGenerator(mix100b, paramMap, env)
val q = qg.generateQuery
q.execute
println(q.toString)
*/

// Testing WorkloadGenerators.stepWorkload
/*
// mix
val mix100 = new MixVector(Map("userByName"->1.0,"b"->0.0,"c"->0.0))

// parameters
var possibleParams = List("user1")
(2 to 10).foreach((i) => {
	possibleParams = ("user" + i) :: possibleParams
})
possibleParams = possibleParams.reverse

val paramMap = Map("userByName"->possibleParams, "b"->List("b"), "c"->List("c"))

val minUsers=1
val maxUsers=20
val nIntervals=4
val durations = List.make(nIntervals, 10*1000)

val wd = WorkloadGenerators.stepWorkload(mix100, paramMap, minUsers, maxUsers, nIntervals, durations, env)
println(wd.workload.toString)

//wd.serialize("/Users/ksauer/Desktop/wd")  // can't do this b/c "Environment" is not serializable
// So, I'll just have to pass in the WorkloadDescription object (rather than just reading it in)
// Later on, I can ask MA if we can make "Environment" serializable
*/


// Testing "userByEmail" query
/*
(1 to 1000).foreach((i) => {
	val u = new user
	u.name("user" + i)
	u.password("secret")
	u.email("user@test.com")
	u.save
})


val startt = System.nanoTime()
val res2 = Queries.userByEmail("user@test.com")
val endt = System.nanoTime()
val latency = endt-startt
//println("Latency = " + (latency/1000000.0))
println(new java.util.Date() + ": executed: userByEmail, start=" + startt + ", end=" + endt + ", latency=" + (latency/1000000.0))
println(Thread.currentThread().getName())  // this should work for getting thread name by which each "get"/"get_set" is executed
*/
/*
println("Answer(0): " + res2.apply(0))
println("Answer(1): " + res2.apply(1))
println()

val res3 = Queries.userByEmail("user@test.com2")
println()
println(res3)
println()
*/


// Testing Histogram class
/*
class Histogram (histFilename: String) {
	var breaks:List[Int] = null
	var counts:List[Int] = null
	
	def readFromFile() = {
		var first = true
		for (line <- Source.fromFile(histFilename).getLines) {
			// Skip first line
			if (first) {
				first = false
			} else {
				val entries = line.split(",")
				breaks = entries(1).toInt :: breaks
				counts = entries(2).toInt :: counts
			}
		}
		
		breaks.reverse
		counts.reverse
	}
	
	def sample():Int = {
		0 // placeholder
	}
}
*/
/*
val h = new Histogram("/Users/ksauer/Desktop/normal_hist_mean10_sd2.csv")
h.readFromFile
println(h.breaks)
println(h.counts)
*/





