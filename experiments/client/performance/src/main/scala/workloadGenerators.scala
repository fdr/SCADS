//package querygen	// no pkg, so I can use MA's scadrclient classes

import java.io._
import java.net._
import scala.io._
import scala.collection.mutable.ListBuffer

import edu.berkeley.cs.scads.model.Environment


object WorkloadGenerators {

	val thinkTime = 10	// ms
	val mix100 = new MixVector(Map("userByName"->1.0,"b"->0.0,"c"->0.0))
	val mix70 = new MixVector(Map("userByName"->0.7, "userByEmail"->0.3))

	// PICK UP HERE!
	// Note:  values of "durations" are in ms
	def stepWorkload(mix:MixVector, parameters: Map[String, List[String]], minUsers:Int, maxUsers:Int, nIntervals:Int, durations:List[Int], env: Environment): WorkloadDescription = {
		val workloadProfile = WorkloadProfile.getLinear(nIntervals, minUsers, maxUsers)
		val queryGenerators = List.make( nIntervals, new SimpleSCADrQueryGenerator(mix, parameters, env) )
		WorkloadDescription.create(workloadProfile, durations, queryGenerators, thinkTime)
	}
	
	def constantWorkload(mix:MixVector, parameters:Map[String, List[String]], nUsers:Int, nIntervals:Int, durations:List[Int], env:Environment): WorkloadDescription = {
		val workloadProfile = WorkloadProfile.getFlat(nIntervals, nUsers)
		val queryGenerators = List.make( nIntervals, new SimpleSCADrQueryGenerator(mix, parameters, env) )
		WorkloadDescription.create(workloadProfile, durations, queryGenerators, thinkTime)
	}
	
	
	
	// can put more as desired.

}


