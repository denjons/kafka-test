package org.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.jboss.arquillian.junit.Arquillian;
import org.kafka.test.client.BasicClient;
import org.wildfly.swarm.arquillian.DefaultDeployment;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;


@RunWith(Arquillian.class)
@DefaultDeployment
public class KafkaProducerTest {

    Thread pollingThread;
    ArrayList<String> messages;

    @After
    public void tearDown(){
        if(pollingThread != null && pollingThread.isAlive()){
            pollingThread.interrupt();
        }
    }

    @Before
    public void setup(){
        messages = new ArrayList<String>();
    }


	@Test
	public void producerEndpointTest() throws InterruptedException {

        BasicClient client = new BasicClient();

        UUID uuid = UUID.randomUUID();
        String group = "kafka-test-group";

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("kafka_test");

        startConsumer(topics, group, uuid);

        for(int i = 0; i < 100; i++){
            Response response = client.postFor(uuid+" "+i, "localhost:8182", MediaType.TEXT_PLAIN_TYPE);
            assertEquals(200, response.getStatus());
        }




        Thread.sleep(1000);

        assertEquals(100, messages.size());

	}

	private void startConsumer(ArrayList<String> topics, String group, UUID testUUID){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092, localhost:9093");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "kafka-test-group");


        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                kafkaConsumer.subscribe(topics);
                try{

                    while(true){
                        ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(100);
                        System.out.println(" received "+consumerRecord.count()+" messages.");
                        consumerRecord.iterator().forEachRemaining(this::feedSubscriber);
                    }

                }finally {
                    kafkaConsumer.close();
                }

            }

            public void feedSubscriber(ConsumerRecord message){
                if(message.value().toString().contains(testUUID.toString())){
                    messages.add(message.value().toString());
                }

            }
        };

        pollingThread = new Thread(runnable);
        pollingThread.start();

    }
}