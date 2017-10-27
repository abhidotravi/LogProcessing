package com.mapr.log4jappender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Log4jAppender which would write to MapR-Streams
 *
 */
public class StreamsLog4jAppender extends AppenderSkeleton {
    private static final String BOOTSTRAP_SERVERS_CONFIG = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String COMPRESSION_TYPE_CONFIG = ProducerConfig.COMPRESSION_TYPE_CONFIG;
    private static final String ACKS_CONFIG = ProducerConfig.ACKS_CONFIG;
    private static final String KEY_SERIALIZER_CLASS_CONFIG = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
    private static final String VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;


    private String brokerList = null;
    private String topic = null;
    private String compressionType = null;
    private String acks = null;
    /*private String keyserializer = null;
    private String valueserializer = null;*/
    private boolean syncSend = false;
    private Producer<String, String> producer = null;

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    /*public String getKeyserializer() {
        return keyserializer;
    }

    public void setKeyserializer(String keyserializer) {
        this.keyserializer = keyserializer;
    }

    public String getValueserializer() {
        return valueserializer;
    }

    public void setValueserializer(String valueserializer) {
        this.valueserializer = valueserializer;
    }*/

    public boolean isSyncSend() {
        return syncSend;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    private Producer<String, String> getKafkaProducer(Properties props) {
        return new KafkaProducer<String, String>(props);
    }


    @Override
    public void activateOptions() {
        // check for config parameter validity
        Properties props = new Properties();
        if (brokerList != null)
            props.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        if (props.isEmpty())
            props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:7645");
        if (topic == null)
            throw new ConfigException("Topic must be specified by the Streams log4j appender");
        if(!topic.startsWith("/",0) && !topic.contains(":"))
            throw new ConfigException("Topic must be specified by the Streams log4j appender as /streamName:topicName");
        if (compressionType != null)
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);
        if(acks != null)
            props.put(ACKS_CONFIG, acks);

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = getKafkaProducer(props);
    }

    @Override
    protected void append(LoggingEvent event) {
        String message = event.getRenderedMessage();
        System.out.println("Message: " + message);
        /*LogLog.debug("[" + new Date(event.getTimeStamp()) + "]" + message);*/
        Future<RecordMetadata> response = producer.send(new ProducerRecord<String, String>(topic, message));
        if (syncSend) {
            try {
                response.get();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            producer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
