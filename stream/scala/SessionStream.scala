package insight.engineer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer



/**
  * Created by sulei on 1/18/18.
  */
case class Session(sessionType: String, startTime: Long, lastClickTime: Long, currentTime: Long, page: String) extends Serializable {}

object SessionStream {

  val sessionTimeout = 1     // In minutes

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(5))
    val hdfs = "hdfs://ip-10-0-0-9.ec2.internal:9000"
    ssc.checkpoint(hdfs + "/checkpoint")

    val kafkaParams = Map[String,Object] (
      "bootstrap.servers" -> "spark://ip-10-0-0-9.ec2.internal:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SessionStream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit"-> (false:java.lang.Boolean)
    )

    val kafkaTopics = Array("log")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferBrokers, Subscribe[String, String](kafkaTopics, kafkaParams))
    val dstream = stream.map(c => c.value())
    val fieldsStream = dstream.transform(rdd => process(rdd))  // fieldsStream: (ip, (timestamp, page))
    val sessions = fieldsStream.updateStateByKey[Session](updateFunc _, new HashPartitioner(10), false)  // sessions: (ip, Session(sessionType, startTime, lastClickTime, currentTime, page))

    sessions.foreachRDD(rdd => {
      val flatRDD = rdd.map(r => (r._1, r._2.sessionType, r._2.startTime, r._2.lastClickTime, r._2.currentTime, r._2.page))
      flatRDD.foreachPartition(it => {
        for(tuple <- it) {
          SqlConnector.del.setString(1, tuple._1)
          SqlConnector.del.setString(2, tuple._2)
          SqlConnector.del.setLong(3, tuple._3)
          SqlConnector.del.setLong(4, tuple._4)
          SqlConnector.del.setLong(5, tuple._5)
          SqlConnector.del.setString(6, tuple._6)
          SqlConnector.del.executeUpdate
        }
      })
    })

    ssc.start()             
    ssc.awaitTermination()  
  }

  def process(rdd:RDD[String]): RDD[(String, (Long, String))] = {
    val lp = new LogParser()
    val records = rdd.map(lp.parse(_)).map(line => {
      val fields = line.split(",")
      (fields(0), (fields(1).toLong, fields(2)))
    })
    return records
  }


  def updateFunc(iter: Iterator[(String, Seq[(Long, String)], Option[Session])]): Iterator[(String, Session)] = {
    val list = ListBuffer[(String, Session)]()
    while(iter.hasNext) {
      val record = iter.next
      val key = record._1
      val newState = updateStateForKey(record._2, record._3)
      val state = newState.getOrElse(Session("Invalid", 0L, 0L, 0L, ""))
      if( state.sessionType != "Invalid") {
        list += ((key, state))                 
      }
    }
    list.toIterator
  }

  // update session based on activity timestamps and last session status
  def updateStateForKey(timestamps: Seq[(Long, String)], state: Option[Session]): Option[Session] = {
    val oldState = state.getOrElse(Session("Invalid", 0L, 0L, 0L, ""))
    val currentTime = System.currentTimeMillis()
    val now = currentTime

    if( oldState.sessionType == "Invalid" ) {                 // New Session if no previous session status
      Some(Session("New", timestamps.head._1, timestamps.last._1, currentTime, timestamps.last._2))
    }
    else if( timestamps == null || timestamps.isEmpty ) {     
      if( oldState.sessionType == "Closed" ) {                // Should the session be removed
        if( (now - oldState.lastClickTime) >= sessionTimeout * 60 * 1000 ) {
          Some(Session("Invalid", 0L, 0L, 0L, ""))
        } else {
          Some(oldState)                        
      } else if( oldState.sessionType == "Open" ) {           // Should the session be closed
        if( (now - oldState.lastClickTime) >= sessionTimeout * 60 * 1000 ) {
          Some(Session("Closed", oldState.startTime, oldState.lastClickTime, currentTime, oldState.page))
        } else {
          Some(oldState)
        }
      } else {
        Some(Session("Open", oldState.startTime, oldState.lastClickTime, currentTime, oldState.page))
      }
    } else {    // keys in current batch
      if( oldState.sessionType == "Closed" ) {                    // Reopen the session as new   
        Some(Session("New", timestamps.head._1, timestamps.last._1, currentTime, timestamps.last._2))
      } else {                                                    
        Some(Session("Open", oldState.startTime, timestamps.last._1, currentTime, timestamps.last._2))
      }
    }
  }
}