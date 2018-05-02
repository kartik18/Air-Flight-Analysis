import org.apache.spark.sql.functions._

object ApplicationUtils {

  def toCapatilize = udf( (deviceName: String) => deviceName.capitalize
  )
}
