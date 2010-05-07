object MainTester extends Application {
	val whichOp = System.getProperty("whichOp").toInt			// only one at a time => take advantage of ||ism using EC2
	println("whichOp=>" + whichOp + "<")
}