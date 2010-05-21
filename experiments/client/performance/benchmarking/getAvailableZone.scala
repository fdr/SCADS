import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

val resp = (new AmazonEC2Client(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))).describeAvailabilityZones(new DescribeAvailabilityZonesRequest())
val zones = resp.getDescribeAvailabilityZonesResult().getAvailabilityZone()
println(zones)
println(zones.size())

var i = 0;
while (i < zones.size()) {
	println(zones.get(i).getZoneName())
	println(zones.get(i).getZoneState())
	i += 1
}

/*
(1 to zones.size()).foreach((i:Int) => {
	println(zones[i])
})
*/