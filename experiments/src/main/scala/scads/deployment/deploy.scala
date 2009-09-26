package scads.deployment

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._
import scads.director._
import performance._

case class SCADSDeployment(
	deploymentName:String
) {
	var scads:Scads = null
	var clients:ScadsClients = null
	var monitoring:SCADSMonitoringDeployment = null
	var director:DirectorDeployment = null
	
	var deployDirectorToMonitoring = true
	
	var experimentsJarURL = "http://scads.s3.amazonaws.com/experiments-1.0-jar-with-dependencies.jar"
	
	/**
	* This will load an existing deployment of scads based on the deployment name
	*/
	/*def loadExistingDeployment {
		scads = Scads.loadState(deploymentName)
		clients = Clients.loadState(deploymentName,scads)
		monitoring = SCADSMonitoringDeployment.loadState(deploymentName,scads)
		director = DirectorDeployment.loadState(deploymentName,scads,monitoring)
	}*/
	
	// parameters to add:
	// how many scads hot-standby's?
	// initial configuration and dataset
	// xtrace sampling probability
	def deploy(nClients:Int, deployMonitoring:Boolean, deployDirector:Boolean) {
		val doMonitoring = if (deployDirector) { true } else { false }
		
		// create the components
		if (doMonitoring) monitoring = SCADSMonitoringDeployment(deploymentName)
		scads = Scads(null,deploymentName,doMonitoring,monitoring)
		if (nClients>0) clients = ScadsClients(scads,nClients)
		if (deployDirector) director = DirectorDeployment(scads,monitoring,deployDirectorToMonitoring)
		
		val components = List[Component](scads,clients,monitoring,director)		
		components.filter(_!=null).foreach(_.boot) 				// boot up all machines
		components.filter(_!=null).foreach(_.waitUntilBooted) 	// wait until all machines booted
		components.filter(_!=null).foreach(_.deploy)			// deploy on all
		components.filter(_!=null).foreach(_.waitUntilDeployed)	// wait until all deployed
	}
	
	def startDirector(policy:Policy)
	
	def startWorkload(workload:WorkloadDescription)
	
	def pullExperimentData
	
	/**
	* Create a page on http://scm/scads/keypair_deploymentName.html that contains important links for this deployment:
	* chukwa ping, performance graphs, director logs, ...
	*/
	def summaryPage
	
	def clientVMs:InstanceGroup = if (clients==null) { new InstanceGroup() } else clients.clients
	def monitoringVM:Instance = if (monitoring==null) null else monitoring.monitoringVM
	def directorVM:Instance = if (director==null) null else director.directorVM
}