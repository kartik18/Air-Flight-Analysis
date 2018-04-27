import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object ImprovedAirFlight {

  val spark = SparkSession.builder()
    .appName("Air Flight Analysis")
    .master("local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header",true)
    .option("inferSchema", "true")
    .load("D:/airline_data/train_df.csv").persist()

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val arrivalFlightDelayed  = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY")< 0)

    val arrivalFlightOnTime = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") === 0)

    val countDelayedByWeekdf = arrivalFlightDelayed.groupBy(df("DAY_OF_WEEK")).count.withColumnRenamed("count","Delayed")

    val countOnTimeDayByWeekdf = arrivalFlightOnTime.groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","On_Time")

    //  val countDelayedForwardByWeekdf = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") > 0 ).groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","Before_Time")

    val countArrivalFlightByWeekdf = df.filter(df("ARR_DELAY") =!= "null").select(df("ARR_DELAY"),df("DAY_OF_WEEK")).groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","Arrival")

    val join1FlightByWeekdf = countDelayedByWeekdf.join(countOnTimeDayByWeekdf,Seq("DAY_OF_WEEK"),"inner").cache()

    val join2FlightByWeekdf = join1FlightByWeekdf.join(countArrivalFlightByWeekdf,Seq("DAY_OF_WEEK"),"inner").cache()
    // val join3FlightByWeekdf = join2FlightByWeekdf.join(AheadOfTimeFlightByWeek,Seq("DAY_OF_WEEK")).cache()
    // join3FlightByWeekdf.orderBy(df("DAY_OF_WEEK")).show()

    val outcomedf = join2FlightByWeekdf.select(join2FlightByWeekdf("DAY_OF_WEEK"),join2FlightByWeekdf("Delayed"),join2FlightByWeekdf("On_Time"),
      join2FlightByWeekdf("Arrival"))
      .withColumn("Delayed_Percentage",round((join2FlightByWeekdf.col("Delayed") / join2FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("On_Time_Percentage",round((join2FlightByWeekdf.col("On_Time") / join2FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("Ratio", round((join2FlightByWeekdf.col("Delayed") / join2FlightByWeekdf.col("On_Time")).cast("double"),2))
    outcomedf.orderBy(df("DAY_OF_WEEK")).show()
    val countFlightArrival = df.filter(df("ARR_DELAY") =!= "null").count()
    val x = arrivalFlightDelayed.count()
    val y = arrivalFlightOnTime.count()
    val percentageDelayed = (x.toDouble / countFlightArrival.toDouble) * 100
    val percentageOnTime = (y.toDouble / countFlightArrival.toDouble) * 100
    //val percentageAheadTime = (arrivalFlightAheadTime.toDouble / countFlightArrival.toDouble) * 100
    println("Total number of flights delayed "+ x)
    println("Total number of flights on time "+ y)
    println("Percentage flight delayed "+ percentageDelayed)
    println("Percentage flight ontime "+ percentageOnTime)
    // println("Percentage flight Ahead of time "+ percentageAheadTime)

    //outcomedf.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("default.Analytics")
    println("Done")

    val stopTime = System.currentTimeMillis()
    println((stopTime - startTime)/ 1000)

  }
}
