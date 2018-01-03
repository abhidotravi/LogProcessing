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

  def main(args: Array[String]): Unit = {
    if (args.length < 2) throw new IllegalArgumentException("You must specify topics to subscribe and the sink table")

    val Array(topics, table) = args
    val brokers = "maprdemo:9092"
    val groupId = "LogConsumer"
    val batchInterval = "2"
    val pollTimeout = "10000"
    val missedLogTable = "/missedLogs"

    val sparkConf = new SparkConf().setAppName("LogStreamProcessor")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.streaming.kafka.consumer.poll.ms " -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val msgDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valDStream = msgDStream.map(msg =>

      /**
        * The row key of the log is designed to contain the following information
        * 1. Timestamp at which the log was produced.
        * 2. Appender's atomic counter
        * 3. Offset of the message
        * 4. Partition from which the message was obtained.
        */

      msg.timestamp() + "-" + msg.key() + "-" + msg.offset() + "-" + msg.partition() + " " + //_id for the message
        msg.value())

    valDStream.print()

    valDStream.foreachRDD(rdd =>
      if(!rdd.isEmpty()) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate

        import com.mapr.db.spark._
        import com.mapr.db.spark.sql._

        def parseLogLine(log: String) = {
          val lineItem = log.trim.split("\\s+")
          val providerItem = log.trim.split("provider-")
          val logItem = LogItem(
            lineItem(0),
            lineItem(1),
            lineItem(2),
            lineItem(3),
            lineItem(4),
            lineItem(5),
            lineItem.drop(1).mkString(" "),
            if(providerItem.length > 1) providerItem.last else " ")

          MapRDBSpark.docToRow(
            MapRDBSpark.newDocument()
              .set(LogFields.ID, logItem.id)
              .set(LogFields.DATE, ODate.parse(logItem.date))
              .set(LogFields.TIME, OTime.parse(logItem.time))
              .set(LogFields.LOGLEVEL, logItem.logLevel)
              .set(LogFields.THREAD, logItem.thread)
              .set(LogFields.LINENUM, logItem.lineNum)
              .set(LogFields.LOG, logItem.log)
              .set(LogFields.PROVIDER, logItem.dataProvider), LogFields.schema)
        }

        val df = spark.createDataFrame(rdd.map(parseLogLine), LogFields.schema)
        //All _id should be unique and should fail if we are re-inserting a row
        //TODO: Handle the failure
        df.write.option("Operation", "Insert").saveToMapRDB(table, LogFields.ID)
      }
    )

    //Start the computation
    println("Start Consuming")
    ssc.start()
    //Wait for the computation to terminate
    ssc.awaitTermination()
  }
}
