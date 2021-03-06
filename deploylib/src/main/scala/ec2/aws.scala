package deploylib.ec2

import java.io.File
import com.amazonaws.services.ec2._
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials

/**
 * Abstract class that is used to get the the accessKeyId and the secretAcccess key from the environmental variables <code>AWS_ACCESS_KEY_ID</code> and <code>AWS_SECRET_ACCESS_KEY</code> respectively.
 * It is used by the EC2Instance object and S3Cache.
 */
trait AWSConnection {
  protected val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  protected val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
  protected def ec2PrivateKey = new File(System.getenv("EC2_PRIVATE_KEY"))
  protected def ec2Cert = new File(System.getenv("EC2_CERT"))
  protected def userID = System.getenv("AWS_USER_ID")

  protected val config = new ClientConfiguration()
  protected val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
}
