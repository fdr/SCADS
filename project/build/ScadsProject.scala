import sbt._
import xsbt.ScalaInstance

import java.io.File

class ScadsProject(info: ProjectInfo) extends ParentProject(info) {

  abstract class ScadsSubProject(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin {
    override def fork = forkRun("-Xmx4G" ::
				"-Djava.library.path=/usr/local/mesos/lib/java/" :: Nil)

    protected def getLocalJars(project: BasicScalaProject): Seq[File] = {
      //HACK: Sbt seems to be missing some the transitive dependencies in lib_managed so we'll just go get them all ourself
      val managedDependencies = (project.managedDependencyPath / "compile" ** "*.jar").getFiles
      val localDependencies = project.dependencies.map(_.asInstanceOf[BasicScalaProject])
       project.jarPath.asFile :: (localDependencies.flatMap(getLocalJars).toList ++ managedDependencies)
    }

    def packagedClasspath = {
      val scalaJars = mainDependencies.scalaJars.getFiles

      val localJars = getLocalJars(this)
      (scalaJars ++ localJars).map(_.getCanonicalPath)
    }

    //Also kind of a hack
    lazy val writePackagedClasspath = task {
      FileUtilities.write(
	new File("classpath"),
	packagedClasspath.mkString(":"),
	log
      )

      FileUtilities.write(
	new File("allJars"),
	packagedClasspath.mkString("\n"),
	log
      )
    } dependsOn(`package`) describedAs("Package classes and API docs.")


    //HACK
    val bdb = "com.sleepycat" % "je" % "4.0.71"
    val optional = "optional" %% "optional" % "0.1"
    val zookeeper = "org.apache.hadoop.zookeeper" % "zookeeper" % "3.3.1"
  }

  class Repl(info: ProjectInfo) extends DefaultWebProject(info) with AvroCompilerPlugin
  {
    val snapshots = ScalaToolsSnapshots
    val lift = "net.liftweb" %% "lift-mapper" % "2.2-SNAPSHOT" % "compile"
    val jetty6 = "org.mortbay.jetty" % "jetty" % "6.1.25" % "test"
    val h2 = "com.h2database" % "h2" % "1.2.121" % "runtime"
    // alternately use derby
    // val derby = "org.apache.derby" % "derby" % "10.2.2.0" % "runtime"
    val servlet = "javax.servlet" % "servlet-api" % "2.5" % "provided"
    val junit = "junit" % "junit" % "3.8.1" % "test"
    val sl4jConfiggy = "com.notnoop.logging" % "slf4j-configgy" % "0.0.1"
  }

  class DeployLib(info: ProjectInfo) extends ScadsSubProject(info) {
    val mesos = "edu.berkeley.cs.mesos" % "java" % "1.0"
    val communication = "edu.berkeley.cs.scads" %% "communication" % "2.1.0-SNAPSHOT"
    val configgy = "net.lag" % "configgy" % "2.0.0"
    val staxApi = "javax.xml.stream" % "stax-api" % "1.0"
    val jaxbApi = "javax.xml.bind" % "jaxb-api" % "2.1"
    val json = "org.json" % "json" % "20090211"
    val ec2 = "com.amazonaws" % "ec2" % "20090404"
    val ganymedSsh2 = "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"
    val commonsLoggingApi = "commons-logging" % "commons-logging-api" % "1.1"
    val commonsHttpClient = "commons-httpclient" % "commons-httpclient" % "3.0.1"
    val jets3t = "net.java.dev.jets3t" % "jets3t" % "0.7.1"
    val jetty = "org.mortbay.jetty" % "jetty" % "6.1.6"
  }


  class Config(info: ProjectInfo) extends DefaultProject(info) {
    val configgy = "net.lag" % "configgy" % "2.0.0"
    val scalaTest = "org.scalatest" % "scalatest" % "1.2"
    val junit = "junit" % "junit" % "4.7"
  }
 class AvroPlugin(info: ProjectInfo) extends DefaultProject(info) {
    val avroJava = "org.apache.hadoop" % "avro" % "1.3.3"
    val configgy = "net.lag" % "configgy" % "2.0.0"
  }
 class Comm(info: ProjectInfo) extends ScadsSubProject(info) {
    val netty = "org.jboss.netty" % "netty" % "3.2.1.Final"
    val log4j = "log4j" % "log4j" % "1.2.15"
  }
  class Piql(info: ProjectInfo) extends ScadsSubProject(info)
  class ScalaEngine(info: ProjectInfo) extends ScadsSubProject(info){
  }
  class Perf(info: ProjectInfo) extends ScadsSubProject(info) {
  }

  lazy val config      = project("config", "config", new Config(_))
  lazy val avro        = project("avro", "avro-plugin", new AvroPlugin(_))
  lazy val comm        = project("comm", "communication", new Comm(_), config, avro)
  lazy val scalaengine = project("scalaengine", "storage-engine", new ScalaEngine(_), config, avro, comm)
  lazy val piql        = project("piql", "piql", new Piql(_), config, avro, comm, scalaengine)
  lazy val perf        = project("perf", "performance", new Perf(_), config, avro, comm, scalaengine, piql, deploylib)

  lazy val deploylib = project("deploylib", "deploylib", new DeployLib(_), comm)
  lazy val repl      = project("repl", "repl", new Repl(_), perf)


  //PIQL Apps
  class Scadr(info: ProjectInfo) extends ScadsSubProject(info) {
  }
  //lazy val scadr       = project("piql" / "scadr", "scadr", new Scadr(_), piql)

  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"
  override def managedStyle = ManagedStyle.Maven
  val publishTo = "Radlab Snapshots" at "http://scads.knowsql.org/nexus/content/repositories/snapshots/"
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)
}

trait AvroCompilerPlugin extends AutoCompilerPlugins {
  override def getScalaInstance(version: String) = {
    val pluginJars = compileClasspath.filter(path => pluginDeps.contains(path.name)).getFiles.toSeq
    withExtraJars(super.getScalaInstance(version), pluginJars)
  }

   private val pluginDeps = Set("avro-1.3.3.jar", "jackson-core-asl-1.4.2.jar", "jackson-mapper-asl-1.4.2.jar")

   private def withExtraJars(si: ScalaInstance, extra: Seq[File]) =
     ScalaInstance(si.version, si.libraryJar, si.compilerJar, info.launcher, extra : _*)


  val avroPlugin = compilerPlugin("edu.berkeley.cs.scads" %% "avro-plugin" % "2.1.0-SNAPSHOT")
  val pluginDepenency = "edu.berkeley.cs.scads" %% "avro-plugin" % "2.1.0-SNAPSHOT"
}
