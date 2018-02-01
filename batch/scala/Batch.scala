package insight.engineer


import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/**
  * Created by sulei on 1/16/18.
  */
case class Record(ip:String, timestamp:Long, page:String)

object Batch {


  def main(args: Array[String]): Unit = {


    //set up the spark configuration and create contexts
    val sparkConf = new SparkConf()
      .setAppName("BatchJob")
      .setMaster("spark://ip-10-0-0-9.ec2.internal:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    //concat input objects in s3
    var names = new ListBuffer[String]()
    for (i <- 0 to 1000) {
      names += (i + ".log")
    }
    val files = names.toList.map(key => "s3a://logexpert/input/" + key).mkString(",")


    // load from s3
    val myFile = sc.textFile(files)

    // parse input
    val records = myFile.map(parse)
    import org.apache.spark.sql.functions._
    val recordsDF = sqlContext.createDataFrame(records)     // recordsDF: (ip, timestamp, page)


    // build session
    import org.apache.spark.sql.expressions.Window
    val windowByIp = Window.partitionBy(recordsDF.col("ip")).orderBy(recordsDF.col("timestamp").asc)
    val sessionDF = recordsDF.withColumn("session", Sessionization.calculateSession(recordsDF.col("timestamp")) over windowByIp)


    // calculate duration for each session
    val windowBySession = Window.partitionBy(sessionDF.col("session")).orderBy(sessionDF.col("timestamp").asc)
    val sessionWithDurationDF = sessionDF.withColumn("duration", sessionDF.col("timestamp") - (min(sessionDF.col("timestamp")) over windowBySession))
    val windowBySessionWithDuration = Window.partitionBy(sessionWithDurationDF.col("session"))
    val sessionWithMaxLengthDF = sessionWithDurationDF.withColumn("maxLength", (max(sessionWithDurationDF.col("duration")) over windowBySessionWithDuration)) // sessionWithMaxLengthDF: (ip, ts, page, session, duration, maxLength)


    // label session
    val sessionWithLabelDF = sessionWithMaxLengthDF.withColumn("success", Label.getLabel(sessionWithMaxLengthDF.col("page")).over(windowBySession))  // sessionWithLabelDF: (ip, timestamp, page, session, duration, maxLength, success)


    // select only final rows for each session
    val spark=sqlContext.sparkSession
    import spark.implicits._
    val output = sessionWithLabelDF.filter($"duration" === $"maxLength").select($"session", $"ip", $"duration", ($"timestamp"-$"duration").alias("start_time"),
      $"timestamp".alias("end_time"), $"success".alias("buy"))  // output: (session, ip, duration, start_time, end_time, buy)

    //write dataframe to database
    output.write.mode(SaveMode.Overwrite).jdbc(Connection.url, Connection.table, Connection.prop)
  }



  def parse(line: String) : Record = {
    val lp = new LogParser()
    val res = lp.parse(line)
    return new Record(res(0), res(1).toLong, res(2))
  }

}
