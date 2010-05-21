//package querygen	// no pkg, so I can use MA's scadrclient classes

import java.io._
import java.net._
//import java.util._
import java.util.Random
import scala.io._
import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Map
import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.model.Environment
import querygen.RandomUser

/*
* Workload Description Classes
*/

@serializable
class WorkloadIntervalDescription(
	val numberOfActiveUsers: Int,
	val duration: Int,
	val queryGenerator: SCADrQueryGenerator
)

/*
@serializable
class WorkloadIntervalDescription(
	val numberOfActiveUsers: Int,
	val duration: Int,
	val requestGenerator: SCADSRequestGenerator)
*/

@serializable
class WorkloadDescription(
	val thinkTimeMean: Long,
	val workload: List[WorkloadIntervalDescription]
	)
{
	def getMaxNUsers(): Int = workload.map(_.numberOfActiveUsers).reduceLeft(Math.max(_,_))

	def serialize(file: String) = {
        val out = new ObjectOutputStream( new FileOutputStream(file) )
        out.writeObject(this);
        out.close();
	}
}

/*
object WorkloadDescription {
	var rnd = new java.util.Random

	def setRndGenerator(r:java.util.Random) { rnd = r }

	def create(workloadProfile:WorkloadProfile, durations:List[Int], requestGenerators:List[SCADSRequestGenerator], thinkTimeMean:Long): WorkloadDescription = {
		assert(workloadProfile.profile.length==durations.length && workloadProfile.profile.length==requestGenerators.length,
			"workloadProfile, durations, and requestGenerators need to have the same length")
		val intervals = workloadProfile.profile.zip(durations).zip(requestGenerators).map( e => new WorkloadIntervalDescription(e._1._1,e._1._2,e._2) )
		new WorkloadDescription(thinkTimeMean,intervals)
	}

	def cat(workloads:List[WorkloadDescription]):WorkloadDescription = new WorkloadDescription(workloads(0).thinkTimeMean,List.flatten( workloads.map(_.workload) ))

	def deserialize(file: String): WorkloadDescription = {
        val fin = new ObjectInputStream( new FileInputStream(file) )
        val wd = fin.readObject().asInstanceOf[WorkloadDescription]
        fin.close
        wd
	}
}
*/

object WorkloadDescription {
	var rdm = new Random
	
	def setRdmGenerator(r: Random) { rdm = r }
	
	def create(workloadProfile: WorkloadProfile, durations: List[Int], queryGenerators: List[SCADrQueryGenerator], thinkTimeMean: Long): WorkloadDescription = {
		assert(workloadProfile.profile.length == durations.length && workloadProfile.profile.length == queryGenerators.length, 
			"workloadProfile, durations, and queryGenerators must have the same length")
		val intervals = workloadProfile.profile.zip(durations).zip(queryGenerators).map( e => new WorkloadIntervalDescription(e._1._1,e._1._2,e._2) )
			new WorkloadDescription(thinkTimeMean,intervals)
	}
	
	def cat(workloads:List[WorkloadDescription]):WorkloadDescription = new WorkloadDescription(workloads(0).thinkTimeMean,List.flatten( workloads.map(_.workload) ))

	def deserialize(file: String): WorkloadDescription = {
        val fin = new ObjectInputStream( new FileInputStream(file) )
        val wd = fin.readObject().asInstanceOf[WorkloadDescription]
        fin.close
        wd
	}	
}

/*
* Workload Profiles
*/


// Add methods as needed
object WorkloadProfile {
	def getFlat(nintervals:Int, nusers:Int): WorkloadProfile = {
		new WorkloadProfile( List.make(nintervals,nusers) )
	}

	def getLinear(nintervals:Int, nusersStart:Int, nusersEnd:Int): WorkloadProfile = {
		val step = (nusersEnd.toDouble-nusersStart)/(nintervals-1)
		new WorkloadProfile( (1 to nintervals).map( (i:Int) => Math.round(nusersStart + (i-1)*step).toInt ).toList )
	}

}

@serializable
case class WorkloadProfile(
	val profile: List[Int]
) {
	private def fraction(a:Double, b:Double, i:Double, min:Double, max:Double):Double = min + (max-min)*(i-a)/(b-a)
	private def interpolateTwo(a:Double, b:Double, n:Int):List[Int] = (0 to n).toList.map( (i:Int)=>Math.round(a + (b-a)*i/n).toInt )

	def addSpike(tRampup:Int, tSpike:Int, tRampdown:Int, tEnd:Int, magnitude:Double): WorkloadProfile = {
		val maxUsers = profile.reduceLeft( Math.max(_,_) ).toDouble
		val w = profile.zipWithIndex.map( e =>  	 if (e._2<tRampup) 				e._1.toDouble
											else if (e._2>=tRampup&&e._2<tSpike) 	e._1 * fraction(tRampup,tSpike,e._2,1,magnitude).toDouble
											else if (e._2>=tSpike&&e._2<tRampdown) 	e._1 * magnitude.toDouble
											else if (e._2>=tRampdown&&e._2<tEnd) 	e._1 * fraction(tRampdown,tEnd,e._2,magnitude,1).toDouble
											else 				 					e._1.toDouble ).toList
		val maxw = w.reduceLeft( Math.max(_,_) ).toDouble
		WorkloadProfile( w.map( (x:Double) => (x/maxw*maxUsers).toInt ) )
	}

	def interpolate(nSegments:Int): WorkloadProfile = {
		WorkloadProfile( List.flatten( profile.dropRight(1).zip(profile.tail).map( (p:Tuple2[Int,Int]) => interpolateTwo(p._1,p._2,nSegments).dropRight(1) ) )+profile.last )
	}

	def scale(nMaxUsers:Int): WorkloadProfile = {
		val maxw = profile.reduceLeft( Math.max(_,_) ).toDouble
		WorkloadProfile( profile.map( (w:Int) => (w/maxw*nMaxUsers).toInt ) )
	}

	override def toString(): String = profile.toString()
}

/*
* Workload Mix Classes
*/
/*
object WorkloadMixProfile {
	def getStaticMix(nintervals:Int, mix:MixVector): WorkloadMixProfile = new WorkloadMixProfile( List.make(nintervals,mix) )
}

@serializable
case class WorkloadMixProfile(
	val profile: List[MixVector]
) {
	def getProfile: List[MixVector] = profile
	def transition(that:WorkloadMixProfile, tStart:Int, tEnd:Int): WorkloadMixProfile =
		new WorkloadMixProfile( profile.zipWithIndex.map( m => m._1.transition(that.getProfile(m._2),if (m._2<tStart) 0 else if (m._2>tEnd) 1 else (m._2.toDouble-tStart)/(tEnd-tStart)) ) )
	override def toString() = profile.map(_.toString).mkString("MIX[",",","]")
}

@serializable
class MixVector(
	_mix: Map[String,Double]
) {
	val mix = normalize(_mix)

	private def sum(m:Map[String,Double]): Double = m.values.reduceLeft(_+_)
	private def normalize(m:Map[String,Double]): Map[String,Double] = m.transform((k,v)=>v/sum(m))
	def getMix: Map[String,Double] = mix

	def transition(that:MixVector,c:Double): MixVector = {
		new MixVector( mix.transform( (k,v) => t(v,that.mix(k),c) ) )
	}
	private def t(a:Double, b:Double, r:Double): Double = a + r*(b-a)
	override def toString() = mix.keySet.toList.sort(_<_).map(k=>k+"->"+mix(k)).mkString("m[",",","]")

	def sampleRequestType(): String = {
		val r = WorkloadDescription.rnd.nextDouble()
		var agg:Double = 0

		var reqType = ""
		for (req <- mix.keySet) {
			agg += mix(req)
			if (agg>=r && reqType=="") reqType = req
		}
		reqType
	}
}
*/

@serializable
class MixVector(
	_mix: Map[String,Double]
) {
	val mix = normalize(_mix)

	private def sum(m:Map[String,Double]): Double = m.values.reduceLeft(_+_)
	private def normalize(m:Map[String,Double]): Map[String,Double] = m.transform((k,v)=>v/sum(m))
	def getMix: Map[String,Double] = mix

	def transition(that:MixVector,c:Double): MixVector = {
		new MixVector( mix.transform( (k,v) => t(v,that.mix(k),c) ) )
	}
	private def t(a:Double, b:Double, r:Double): Double = a + r*(b-a)
	override def toString() = mix.keySet.toList.sort(_<_).map(k=>k+"->"+mix(k)).mkString("m[",",","]")

	def sampleQueryType(): String = {
		val r = WorkloadDescription.rdm.nextDouble
		var agg:Double = 0

		var queryType = ""
		for (query <- mix.keySet) {
			agg += mix(query)
			if (agg>=r && queryType=="") queryType = query
		}
		queryType
	}
}


/*
* Query Parameter Generators
*/
/*
object SCADSKeyGenerator {
	def getMinKey(gens:List[SCADSKeyGenerator]):Int = gens.map(_.minKey).reduceLeft(Math.min(_,_))
	def getMaxKey(gens:List[SCADSKeyGenerator]):Int = gens.map(_.maxKey).reduceLeft(Math.max(_,_))

	def wikipediaKeyProfile(nintervals:Int, dataset:String, nHoursSkip:Int, nHoursDuration:Int, nKeys:Int, keyHour:Int): List[SCADSKeyGenerator] = {
		val keyhourURL = Source.fromURL(WikipediaDataset.datasets(dataset)).getLines.toList.map(_.trim)(keyHour)
		val keys = Set[String]() ++ WikipediaDataset.loadPagesAndHits(keyhourURL, nKeys, null).keySet
		val hours = Source.fromURL(WikipediaDataset.datasets(dataset)).getLines.toList.map(_.trim).drop(nHoursSkip).take(nHoursDuration)
		val hourGenerators = hours.map( WikipediaKeyGenerator(_,keys) )
		val nPerHour = Math.ceil(nintervals.toDouble/(nHoursDuration-1)).toInt
		val keyProfile = List.flatten( hourGenerators.dropRight(1).zip(hourGenerators.tail).map( (g)=> (0 to (nPerHour-1)).toList.map( (i:Int)=> MixtureKeyGenerator(Map(g._1->(1.0-i/nPerHour.toDouble),g._2->(i/nPerHour.toDouble)) )) ) )+hourGenerators.last
		keyProfile.take(nintervals)
	}

	def sampleKeys(generators:List[SCADSKeyGenerator], nsamples:Int, file:String) {
		val f = new BufferedWriter(new FileWriter(file))
		for (gen <- generators)	f.write( (1 to nsamples).toList.map( i=>gen.generateKey ).mkString("",",","\n") )
		f.close
	}

}

@serializable
abstract class SCADSKeyGenerator(
	val minKey: Int,
	val maxKey: Int
) {
	def generateKey(): Int
}

@serializable
case class UniformKeyGenerator(
	override val minKey: Int,
	override val maxKey: Int
) extends SCADSKeyGenerator(minKey,maxKey) {
	override def generateKey(): Int = WorkloadDescription.rnd.nextInt(maxKey-minKey) + minKey
}

@serializable
case class MixtureKeyGenerator(
	components: Map[SCADSKeyGenerator,Double]
) extends SCADSKeyGenerator(SCADSKeyGenerator.getMinKey(components.keySet.toList),SCADSKeyGenerator.getMaxKey(components.keySet.toList)) {
	override def generateKey(): Int = {
		var (s,r) = (0.0,WorkloadDescription.rnd.nextDouble)
		(for (c<-components) yield {s+=c._2;(c._1,s)}).find(r<_._2).get._1.generateKey
	}
}
*/

abstract class SCADrParamGenerator(val possibleParams: List[String]) {
	def generateParam(): String
}

@serializable
case class UniformParamGenerator(override val possibleParams: List[String]) extends SCADrParamGenerator(possibleParams) {
	override def generateParam(): String = {
		val len = possibleParams.length
		val rdm = new RandomUser(len)
		val paramIdx = rdm.getRandomUser
		
		return possibleParams(paramIdx)
	}
}

/**
* a: shape of the Zipf dist'n, larger a implies dist'n with taller peak
* r: random number used to shuffle keys around
*/
/*
@serializable
case class ZipfKeyGenerator(
	val a: Double,
	val r: Double,
	override val minKey: Int,
	override val maxKey: Int
) extends SCADSKeyGenerator(minKey,maxKey) {
	assert(a>1, "need a>1")

	override def generateKey(): Int = {
		var k = -1
		do { k=sampleZipf } while (k>maxKey)
		Math.abs( ((k+1)*r).hashCode ) % (maxKey-minKey) + minKey
	}

	private def sampleZipf(): Int = {
		val b = Math.pow(2,a-1)
		var u, v, x, t = 0.0
		do {
			u = WorkloadDescription.rnd.nextDouble()
			v = WorkloadDescription.rnd.nextDouble()
			x = Math.floor( Math.pow(u,-1/(a-1)) )
			t = Math.pow(1+1/x,a-1)
		} while ( v*x*(t-1)/(b-1)>t/b )
		x.toInt
	}
}
*/


/*
* SCADr Queries
*/
// REVIEW scadr.piql and decide what queries to start with
// Can always add on later
// Might not hurt to just get everything working with a single query type
abstract class SCADrQuery {
	def queryType: String
	def execute
}

/*
abstract class SCADrQuery (
	implicit val env: Environment
){
	def queryType: String
	def execute
}*/

// works now that "env" is 2nd; didn't work when "env" was 1st
case class SCADrQueryUserByName (val username: String, implicit val env: Environment) extends SCADrQuery() {
	def queryType: String = "userByName"
	
	def execute = {
		/*
		//val res = Queries.userByName(username).apply(0).name
		val res = Queries.userByName(username)
		res  // bc "block must end in result expression"
		*/
		Queries.userByName(username)
	}
	
	override def toString: String = "userByName(" + username + ")"
}

case class SCADrQueryUserByEmail (val email: String, implicit val env: Environment) extends SCADrQuery() {
	def queryType: String = "userByEmail"
	
	def execute = {
		/*
		val res = Queries.userByEmail(email)
		res
		*/
		Queries.userByEmail(email)
	}
	
	override def toString: String = "userByEmail(" + email + ")"
}

case class SCADrQueryThoughtstream (val username: String, val numThoughts: Int, implicit val env: Environment) extends SCADrQuery {
	def queryType: String = "thoughtstream"
	
	def execute = {
		/*
		val res = Queries.userByName(username).apply(0).thoughtstream(numThoughts)
		res
		*/
		Queries.userByName(username).apply(0).thoughtstream(numThoughts)
	}
	
	override def toString: String = "thoughtstream(" + username + "," + numThoughts + ")"
}

case class SCADrQueryThoughtsByHashTag (val tag: String, val numThoughts: Int, implicit val env: Environment) extends SCADrQuery {
	def queryType: String = "thoughtsByHashTag"
	
	def execute = {
		Queries.thoughtsByHashTag(tag, numThoughts)
	}
	
	override def toString: String = "thoughtsByHashTag(" + tag + "," + numThoughts + ")"
}


/*
* SCADr Query Generators
*/

@serializable
abstract class SCADrQueryGenerator (val mix: MixVector) {
	def generateQuery: SCADrQuery
}

@serializable
class SimpleSCADrQueryGenerator (
	override val mix: MixVector, 
	val parameters: Map[String, List[String]], 
	implicit val env: Environment
) extends SCADrQueryGenerator(mix) {  
	// parameters:  each query type has its own list of possible parameters
	val userByNameParamGenerator = new UniformParamGenerator(parameters("userByName"))
	val userByEmailParamGenerator = new UniformParamGenerator(parameters("userByEmail"))
	val thoughtsByHashTagParamGenerator = new UniformParamGenerator(parameters("thoughtsByHashTag"))
	
	// add on as I have classes for more queries implemented -- will have more param generators and more alternatives
	// in the "match" stm
	
	def generateQuery: SCADrQuery = {
		val numThoughtsToDisplay = parameters("thoughtstream")(0).toInt
		
		mix.sampleQueryType match {
			case "userByName" => new SCADrQueryUserByName(userByNameParamGenerator.generateParam, env)
			case "userByEmail" => new SCADrQueryUserByEmail(userByEmailParamGenerator.generateParam, env)
			case "thoughtstream" => new SCADrQueryThoughtstream(userByNameParamGenerator.generateParam, 
				numThoughtsToDisplay, env)
			case "thoughtsByHashTag" => new SCADrQueryThoughtsByHashTag(thoughtsByHashTagParamGenerator.generateParam,
				numThoughtsToDisplay, env)
			//case _ =>
		}
	}
}

/*
@serializable
abstract class SCADSRequestGenerator(
	val mix: MixVector
) {
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest
}


@serializable
class SimpleSCADSRequestGenerator(
	override val mix: MixVector,
	val parameters: Map[String, Map[String, String]]
) extends SCADSRequestGenerator(mix) {
	val keyFormat = new java.text.DecimalFormat("000000000000000")

	val getKeyGenerator = new UniformKeyGenerator(parameters("get")("minKey").toInt,parameters("get")("maxKey").toInt)
	val getNamespace = parameters("get")("namespace")

	val putKeyGenerator = new UniformKeyGenerator(parameters("put")("minKey").toInt,parameters("put")("maxKey").toInt)
	val putNamespace = parameters("put")("namespace")

	val getsetKeyGenerator = new UniformKeyGenerator(parameters("getset")("minKey").toInt,parameters("getset")("maxKey").toInt)
	val getsetNamespace = parameters("getset")("namespace")
	val getsetSetLength = parameters("getset")("setLength").toInt

	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		mix.sampleRequestType match {
			case "get" => new SCADSGetRequest(client,getNamespace,keyFormat.format(getKeyGenerator.generateKey))
			case "put" => new SCADSPutRequest(client,putNamespace,keyFormat.format(putKeyGenerator.generateKey),"value")
			case "getset" => {
				val startKey = getsetKeyGenerator.generateKey.toInt
				val endKey = startKey+getsetSetLength
				new SCADSGetSetRequest(client,getsetNamespace,keyFormat.format(startKey),keyFormat.format(endKey),0,getsetSetLength)
			}
		}
	}
}

@serializable
case class FixedSCADSRequestGenerator(
	override val mix: MixVector,
	val keyGenerator: SCADSKeyGenerator,
	val namespace: String,
	val getsetRangeLength: Int
) extends SCADSRequestGenerator(mix) {
	val keyFormat = new java.text.DecimalFormat("000000000000000")

	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		mix.sampleRequestType match {
			case "get" => new SCADSGetRequest(client,namespace,keyFormat.format(keyGenerator.generateKey))
			case "put" => new SCADSPutRequest(client,namespace,keyFormat.format(keyGenerator.generateKey),"value")
			case "getset" => {
				val startKey = keyGenerator.generateKey
				val endKey = startKey+getsetRangeLength
				new SCADSGetSetRequest(client,namespace,keyFormat.format(startKey),keyFormat.format(endKey),0,getsetRangeLength)
			}
		}
	}
}
*/