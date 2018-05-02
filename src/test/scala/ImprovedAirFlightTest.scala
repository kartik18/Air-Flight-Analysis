import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import ImprovedAirFlight._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class ImprovedAirFlightTest extends FunSuite with BeforeAndAfterAll{

  test(" Test whether we get count ") {
    assert(func(df).isRight)
  }

}
