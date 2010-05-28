package edu.berkeley.cs.scads.benchmarking

import org.apache.commons.cli._

object TestBoolean {
	def main(args:Array[String]) {
		/*
		val oneBin = System.getProperty("oneBin").toBoolean
		println("oneBin=>" + oneBin + "<")

		if (oneBin)
			println("boolean!")
		*/

		val options = new Options();
		options.addOption("oneBin", true, "indicates that you only want one bin")

		val parser = new GnuParser();
		val cmd = parser.parse(options, args);

		val oneBin = cmd.getOptionValue("oneBin").toBoolean
		if (oneBin)
			println("ok!")
	}
}