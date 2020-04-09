package com.nazjara;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;

import java.util.concurrent.ExecutionException;

public class Application {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "defaultConsumerGroup";
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        launchProducer();
        launchConsumer(consumerGroup);
    }

    private static void launchProducer() {
        Producer<Long, String> kafkaProducer = KafkaProducerService.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            KafkaProducerService.produceMessages(10, kafkaProducer, TOPIC);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private static void launchConsumer(String consumerGroup) {
        System.out.println("Consumer is part of consumer group " + consumerGroup);

        Consumer<Long, String> kafkaConsumer = KafkaConsumerService.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        KafkaConsumerService.consumeMessages(TOPIC, kafkaConsumer);
    }
}