# MapR Event Streams Log4j Appender
Appender that can write logs to MapR Event Streams

### 1. Clone the project and build

```
mvn clean install
```

### 2. Add jar to classpath

Add the `mapr-log4j-appender-1.0-SNAPSHOT.jar` generated in `target` folder to your classpath.

### 3. Make sure you create a Stream to collect logs

Application logs will be directed to MapR-ES. Hence we have to first create an (Event Stream)[https://maprdocs.mapr.com/home/MapR_Streams/getting_started_with_mapr_streams.html]

**Example using (maprcli)[https://maprdocs.mapr.com/home/ReferenceGuide/maprcli-REST-API-Syntax.html]**

Let's create an event stream `LogStream` with _public_ permissions so that anyone can _produce to_ / _consume from_ / _create topics_ in the event streams.

```
maprcli stream create -path /LogStream:App -defaultpartitions 10 -produceperm p -consumeperm p -topicperm p
```
>Usage:

>stream create
	 -path `<stream path>`
 `[ -ttl Time to live in seconds. default:604800 ]`
 `[ -autocreate Auto create topics. default:true ]`
 `[ -defaultpartitions Default partitions per topic. default:1 ]`
 `[ -compression off|lz4|lzf|zlib. default:inherit from parent directory ]`
 `[ -produceperm Producer access control expression. default u:creator ]`
 `[ -consumeperm Consumer access control expression. default u:creator ]`
 `[ -topicperm Topic CRUD access control expression. default u:creator ]`
 `[ -copyperm Stream copy access control expression. default u:creator ]`
 `[ -adminperm Stream administration access control expression. default u:creator ]`
 `[ -copymetafrom Stream to copy attributes from. default:none ]`
 `[ -ischangelog Stream to store changelog. default:false ]`

### 4. Add the following lines to log4j.properties for your application
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
