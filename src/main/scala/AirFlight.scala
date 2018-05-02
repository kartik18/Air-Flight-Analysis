import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object AirFlight {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName("Air Flight Analysis")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header",true)
      .option("inferSchema", "true")
      .load("D:/airline_data/train_df.csv")

    //df.printSchema()

    val arrivalFlightDelayed  = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY")< 0).count()
   // val arrivalFlightDelayeddf = spark.sparkContext.parallelize(Seq(arrivalFlightDelayed))
    //println("No. of Flights that arrived delay "+ arrivalFlightDelayed)

    val arrivalFlightOnTime = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") === 0).count()
    val arrivalFlightAheadTime = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") > 0).count()
    //println("No. of Flights that arrived on Time " + arrivalFlightOnTime)

    val countDelayedByWeekdf = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") < 0 ).groupBy(df("DAY_OF_WEEK")).count

    val DelayedFlightByWeek = countDelayedByWeekdf.withColumnRenamed("count","Delayed")
    //DelayedFlightByWeek.show()

    val countOnTimeDayByWeekdf = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") === 0 ).groupBy(df("DAY_OF_WEEK")).count()
    val OnTimeFlightByWeek = countOnTimeDayByWeekdf.withColumnRenamed("count","On_Time")
      //OnTimeFlightByWeek.show()

    val countDelayedForwardByWeekdf = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") > 0 ).groupBy(df("DAY_OF_WEEK")).count()
    val AheadOfTimeFlightByWeek = countDelayedForwardByWeekdf.withColumnRenamed("count","Before_Time")

    val countArrivalFlightByWeekdf = df.filter(df("ARR_DELAY") =!= "null").select(df("ARR_DELAY"),df("DAY_OF_WEEK")).groupBy(df("DAY_OF_WEEK")).count()
    val ArrivalFlightByWeekdf = countArrivalFlightByWeekdf.withColumnRenamed("count","Arrival")
        //ArrivalFlightByWeekdf.show()

    //println("Count by week")

    val join1FlightByWeekdf = DelayedFlightByWeek.join(OnTimeFlightByWeek,Seq("DAY_OF_WEEK")).cache()
    val join2FlightByWeekdf = join1FlightByWeekdf.join(ArrivalFlightByWeekdf,Seq("DAY_OF_WEEK")).cache()
    val join3FlightByWeekdf = join2FlightByWeekdf.join(AheadOfTimeFlightByWeek,Seq("DAY_OF_WEEK")).cache()
   // join3FlightByWeekdf.orderBy(df("DAY_OF_WEEK")).show()

    val outcomedf = join3FlightByWeekdf.select(join3FlightByWeekdf("DAY_OF_WEEK"),join3FlightByWeekdf("Delayed"),join3FlightByWeekdf("On_Time")
    ,join3FlightByWeekdf("Before_Time"),join3FlightByWeekdf("Arrival"))
      .withColumn("Delayed_Percentage",round((join3FlightByWeekdf.col("Delayed") / join3FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("On_Time_Percentage",round((join3FlightByWeekdf.col("On_Time") / join3FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("Ratio", round((join3FlightByWeekdf.col("Delayed") / join3FlightByWeekdf.col("On_Time")).cast("double"),2))

     outcomedf.orderBy(df("DAY_OF_WEEK")).show()
    //outcomedf.printSchema()
    val countFlightArrival = df.filter(df("ARR_DELAY") =!= "null").count()
    val percentageDelayed = (arrivalFlightDelayed.toDouble / countFlightArrival.toDouble) * 100
    val percentageOnTime = (arrivalFlightOnTime.toDouble / countFlightArrival.toDouble) * 100
    val percentageAheadTime = (arrivalFlightAheadTime.toDouble / countFlightArrival.toDouble) * 100
   // println("Percentage flight delayed "+ percentageDelayed)
    //println("Percentage flight ontime "+ percentageOnTime)
     // println("Percentage flight Ahead of time "+ percentageAheadTime)

    /* spark.sql("create table default.Analytics (DAY_OF_WEEK string,Delayed int,On Time int,Before Time int,Arrival int," +
       "Delayed Percentage float,On Time Percentage float,Ratio float)") */

   //outcomedf.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("default.Analytics")
    println("Done")
    val stopTime = System.currentTimeMillis()
    println((stopTime - startTime)/ 1000)

  }
}
