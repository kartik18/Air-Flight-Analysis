import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import exceptions._

import scala.util.{Failure, Success, Try}

object ImprovedAirFlight  {

  val spark = SparkSession.builder()
    .appName("Air Flight Analysis")
    .master("local")
    .getOrCreate()

  var df:DataFrame = spark.emptyDataFrame

  try {
    df = spark.read.format("csv")
      .option("header",true)
      .option("inferSchema", "true")
      .load("D:/airline_data/train_df.csv")
  }
  catch {
    case  e:org.apache.spark.sql.AnalysisException => println("You have entered wrong file path")
  }

  def func(df:DataFrame):Either[Throwable,Long] = {
    val count =

      Try(df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY")< 0).count()) match {
      case Success(a) => Right(a)


      case Failure(b) => Left(b)
    }
    count
  }

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val arrivalFlightDelayed  = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY")< 0)

    val arrivalFlightOnTime = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") === 0)

    val countDelayedByWeekdf = arrivalFlightDelayed.groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","Delayed")

    val countOnTimeDayByWeekdf = arrivalFlightOnTime.groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","On_Time")

    //  val countDelayedForwardByWeekdf = df.filter(df("ARR_DELAY") =!= "null").filter(df("ARR_DELAY") > 0 ).groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","Before_Time")

    val countArrivalFlightByWeekdf = df.filter(df("ARR_DELAY") =!= "null").select(df("ARR_DELAY"),df("DAY_OF_WEEK")).groupBy(df("DAY_OF_WEEK")).count().withColumnRenamed("count","Arrival")

    val join1FlightByWeekdf = countDelayedByWeekdf.join(countOnTimeDayByWeekdf,Seq("DAY_OF_WEEK"),"inner").cache()

    val join2FlightByWeekdf = join1FlightByWeekdf.join(countArrivalFlightByWeekdf,Seq("DAY_OF_WEEK"),"inner").cache()
    // val join3FlightByWeekdf = join2FlightByWeekdf.join(AheadOfTimeFlightByWeek,Seq("DAY_OF_WEEK")).cache()
    // join3FlightByWeekdf.orderBy(df("DAY_OF_WEEK")).show()

    var outcomedf = spark.emptyDataFrame

    try {

       outcomedf = join2FlightByWeekdf.select(join2FlightByWeekdf("DAY_OF_WEEK"),join2FlightByWeekdf("Delayed"),join2FlightByWeekdf("On_Time"),
      join2FlightByWeekdf("Arrival"))
      .withColumn("Delayed_Percentage",round((join2FlightByWeekdf.col("Delayed") / join2FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("On_Time_Percentage",round((join2FlightByWeekdf.col("On_Time") / join2FlightByWeekdf.col("Arrival")) * 100,2))
      .withColumn("Ratio", round((join2FlightByWeekdf.col("Delayed") / join2FlightByWeekdf.col("On_Time")).cast("double"),2))

      val ans = outcomedf.columns
      val validate = List("DAY_OF_WEEK","Delayed","On_Time","Arrival","Delayed_Percentage","On_Time_Percentage","Ratio")

      val outcome = ans.map(i => i.contains(validate)) //outcome contains false

      if(outcome.contains(false)){
        println("Columns are validated")
      }
      else throw new ColumnsValidateException("Required columns are not found")

    }
    catch {
      case e : ColumnsValidateException => e.printStackTrace()
    }

    outcomedf.orderBy(df("DAY_OF_WEEK")).show()

    val countFlightArrival = df.filter(df("ARR_DELAY") =!= "null").count()
    val countFlight = 0
    val x = arrivalFlightDelayed.count()
    val y = arrivalFlightOnTime.count()
    var percentageDelayed = 0.0d //(x.toDouble / countFlightArrival.toDouble) * 100
    val percentageOnTime = (y.toDouble / countFlightArrival.toDouble) * 100
    try{
      percentageDelayed = (x.toDouble / countFlight.toDouble) * 100

      if(percentageDelayed.isInfinite){
        throw new InfiniteValidateException("Error, percentageOnTime value is infinite")
      }
    }
    catch {
      //case e: java.lang.ArithmeticException => println("For percentage delayed, you cannot divide by zero")
      case c: InfiniteValidateException => c.printStackTrace()
      case _: Throwable => println("You got an error")
    }

    //val percentageAheadTime = (arrivalFlightAheadTime.toDouble / countFlightArrival.toDouble) * 100
    println("Total number of flights delayed "+ x)
    println("Total number of flights on time "+ y)
    println("Percentage flight delayed "+ percentageDelayed)
    println("Percentage flight ontime "+ percentageOnTime)
    //println("Percentage flight Ahead of time "+ percentageAheadTime)

    //outcomedf.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("default.Analytics")
    println("Done")

    val stopTime = System.currentTimeMillis()
    println((stopTime - startTime)/ 1000)



  }
}
