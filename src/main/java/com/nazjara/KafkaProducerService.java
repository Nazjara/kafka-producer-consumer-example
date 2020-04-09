package com.nazjara;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerService {

    public static void produceMessages(long numberOfMessages, Producer<Long, String> kafkaProducer, String topic) throws ExecutionException, InterruptedException {
        int partition = 1;

        for (long i = 0; i < numberOfMessages; i++) {
            String value = String.format("event %d", i);

            long timeStamp = System.currentTimeMillis();

//            choose partition by hashing key
//            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, value);

//            choose partition by round robin
//            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, value);

//            fixed partition
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, partition, timeStamp, i, value);

            RecordMetadata recordMetadata = kafkaProducer.send(record).get();

            System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, offset: %d",
                    record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset()));
        }
    }

    public static Producer<Long, String> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}