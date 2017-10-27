# MapR Streams Log4j Appender
Appender that can write logs to MapR Streams


### Sample log4j.properties configuration
```
log4j.rootLogger=INFO, streams
log4j.appender.streams=com.mapr.log4jappender.StreamsLog4jAppender
log4j.appender.streams.layout=org.apache.log4j.PatternLayout
log4j.appender.streams.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
log4j.appender.streams.brokerList=localhost:1234
# Topic in the following format /<stream_name>:<topic_name>
log4j.appender.streams.topic=/LogStream:AppLogger
log4j.appender.streams.acks=1
log4j.appender.streams.syncSend=false
```