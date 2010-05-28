package edu.berkeley.cs.scads.benchmarking

import org.apache.log4j._

object TestLog4j {
	def main(args:Array[String]) {
		val logger = Logger.getLogger("kristalLogger")
		
		val otherLogger = Logger.getLogger("ryanLogger")

		val filename = "/Users/ksauer/Desktop/log4jConfig.txt"
		PropertyConfigurator.configure(filename)

		logger.info("hi how are you")
		
		otherLogger.debug("great how are you")
	}
}