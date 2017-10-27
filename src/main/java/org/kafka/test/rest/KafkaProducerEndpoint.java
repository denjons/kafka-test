package org.kafka.test.rest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafka.test.producer.ProducerProperties;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/test")
public class KafkaProducerEndpoint {

	static int roundRobin = 0;

	private static String TOPIC = "kafka_test";

	@Inject
    ProducerProperties properties;


	@GET
	@Path("/ping")
	@Produces(MediaType.TEXT_PLAIN)
	public Response testMessage() {

		return Response.ok("method service is running").build();
	}

	@POST
	@Path("/post")
	@Consumes(MediaType.TEXT_PLAIN)
	public void postClichedMessage(String message) {
		System.out.println("Sending message");

		KafkaProducer producer = new KafkaProducer(properties.getProperties());

		ProducerRecord record = new ProducerRecord(TOPIC, String.valueOf(roundRobin), "Some message :)");
		roundRobin ++;

		try{
			producer.send(record);
			System.out.println("message sent to topic: "+TOPIC+", round robin: "+roundRobin);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			producer.close();
		}
	}
}