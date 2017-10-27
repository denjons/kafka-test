package org.kafka.test.producer;

import javax.enterprise.context.ApplicationScoped;
import java.util.Properties;


@ApplicationScoped
public class ProducerProperties {

    Properties properties = new Properties();

    public ProducerProperties(){
        properties.setProperty("bootstrap.servers", "localhost:9092, localhost:9093");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    }

    public Properties getProperties() {
        return properties;
    }
}
