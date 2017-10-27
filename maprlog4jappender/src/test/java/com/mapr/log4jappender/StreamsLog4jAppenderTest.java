package com.mapr.log4jappender;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * Unit tests for StreamsLog4jAppender
 */
public class StreamsLog4jAppenderTest extends TestCase {

    Logger logger = Logger.getLogger(StreamsLog4jAppender.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public StreamsLog4jAppenderTest (String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( StreamsLog4jAppenderTest.class );
    }


    public void testUnitializedStreamsAppender() {
        try {
            PropertyConfigurator.configure(getLog4jConfigBasic());
            Assert.fail("Missing properties exception was expected !");
        } catch (ConfigException ex) {
            Assert.assertEquals("Topic must be specified by the Streams log4j appender", ex.getMessage());
        }
    }

    public void testInvalidStreamsTopicAppender() {
        Properties logProps = getLog4jConfigBasic();
        logProps.put("log4j.appender.streams.brokerList", "localhost:9092");
        logProps.put("log4j.appender.streams.topic", "kafkalogger");

        try {
            PropertyConfigurator.configure(logProps);
            Assert.fail("Missing properties exception was expected !");
        } catch (ConfigException ex) {
            Assert.assertEquals("Topic must be specified by the Streams log4j appender as /streamName:topicName", ex.getMessage());
        }
    }

    public void testLog4jAppends() {
        Properties logProps = getLog4jConfigFull();
        PropertyConfigurator.configure(logProps);
        /*for (int i = 1; i <= 5; ++i) {
            logger.error("2017-10-27 12:35:09 INFO  QueryDebuggingDemo:45 - Query Condition: ((stars > {\"$numberLong\":3}) and (state = \"NV\"))");
        }*/
    }

    private Properties getLog4jConfigBasic() {
        Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO, streams");
        props.put("log4j.appender.streams", "com.mapr.log4jappender.StreamsLog4jAppender");
        props.put("log4j.appender.streams.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.streams.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
        return props;
    }

    private Properties getLog4jConfigFull() {
        Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO, streams");
        props.put("log4j.appender.streams", "com.mapr.log4jappender.StreamsLog4jAppender");
        props.put("log4j.appender.streams.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.streams.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
        props.put("log4j.appender.streams.brokerList", "localhost:1234");
        props.put("log4j.appender.streams.topic", "/stream1:topic1");
        props.put("log4j.appender.streams.acks", "1");
        props.put("log4j.appender.streams.syncSend", "true");
        return props;
    }

}
