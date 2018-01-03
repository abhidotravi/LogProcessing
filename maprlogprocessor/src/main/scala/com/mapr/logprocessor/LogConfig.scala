package com.mapr.logprocessor

/**
  * Created by aravi on 11/8/17.
  */
import org.apache.spark.sql.types._

private[logprocessor] case class LogItem(id: String,
                                         date: String,
                                         time: String,
                                         logLevel: String,
                                         thread: String,
                                         lineNum: String,
                                         log: String = "",
                                         dataProvider: String = " ") extends Serializable

private[logprocessor] object LogFields {
  val ID = "_id"
  val DATE = "date"
  val TIME = "time"
  val LOGLEVEL = "logLevel"
  val THREAD = "thread"
  val LINENUM = "lineNum"
  val LOG = "log"
  val PROVIDER = "dataProvider"

  val schema = StructType(
    StructField(ID, StringType, false) ::
      StructField(DATE, DateType, true) ::
      StructField(TIME, TimestampType, true) ::
      StructField(LOGLEVEL, StringType, true) ::
      StructField(THREAD, StringType, true) ::
      StructField(LINENUM, StringType, true) ::
      StructField(LOG, StringType, true) ::
      StructField(PROVIDER, StringType, true) ::
      Nil)
}