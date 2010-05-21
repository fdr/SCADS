import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter

object TestCmdLineArgParser {
	def main(args: Array[String]) {
		val options = new Options();
		options.addOption("whichOp", true, "specify operator to benchmark")

		val parser = new GnuParser();
		val cmd = parser.parse(options, args);

		if(cmd.hasOption("whichOp"))
			println("Benchmark op " + cmd.getOptionValue("whichOp"))
	}
}