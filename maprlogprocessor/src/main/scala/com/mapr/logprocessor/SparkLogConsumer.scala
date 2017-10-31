package com.mapr.logprocessor

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ojai.types.{ODate, OTime}
import org.apache.spark.sql.types._

/**
  * Created by aravi on 10/29/17.
  */
object SparkLogConsumer extends Serializable {

  case class LogItem(id: String,
                     date: String,
                     time: String,
                     logLevel: String,
                     //thread: String,
                     log: String = "") extends Serializable

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify topics to subscribe and the sink table")
    }

    val Array(topics, table) = args
    val brokers = "maprdemo:9092" //Dummy value for MapR Streams
    val groupId = "LogConsumer"
    val batchInterval = "2"
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("LogStream")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.streaming.kafka.consumer.poll.ms " -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val msgDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valDStream: DStream[String] = msgDStream.map(msg => msg.key() + " " + msg.value())
    valDStream.print()

    valDStream.foreachRDD(rdd =>
      if(!rdd.isEmpty()) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate

        import spark.implicits._
        import org.apache.spark.sql.functions._
        import com.mapr.db.spark._
        import com.mapr.db.spark.sql._

        val schema = StructType(
          StructField(LogConfig.ID, StringType, false) ::
          StructField(LogConfig.DATE, DateType, true) ::
          StructField(LogConfig.TIME, TimestampType, true) ::
          StructField(LogConfig.LOGLEVEL, StringType, true) ::
          StructField(LogConfig.LOG, StringType, true) ::
          Nil)

        def parseLogLine(log: String): Row = {
          val lineItem = log.trim.split("\\s+")
          val logItem = LogItem(
            lineItem(0),
            lineItem(1),
            lineItem(2),
            lineItem(3),
            lineItem.drop(1).mkString(" "))

          MapRDBSpark.docToRow(
            MapRDBSpark.newDocument()
              .set(LogConfig.ID, logItem.id)
              .set(LogConfig.DATE, ODate.parse(logItem.date))
              .set(LogConfig.TIME, OTime.parse(logItem.time))
              .set(LogConfig.LOGLEVEL, logItem.logLevel)
              .set(LogConfig.LOG, lineItem.mkString(" ")), schema)
        }

        val df = spark.createDataFrame(rdd.map(parseLogLine), schema)
        /*df.show
        df.printSchema()*/
        df.write.option("Operation", "Insert").saveToMapRDB(table, LogConfig.ID)
      }
    )

    //Start the computation
    println("Start Consuming")
    ssc.start()
    //Wait for the computation to terminate
    ssc.awaitTermination()
  }
}
