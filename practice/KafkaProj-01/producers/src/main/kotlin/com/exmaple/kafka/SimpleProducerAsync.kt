package com.exmaple.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

fun main() {
    val logger = LoggerFactory.getLogger("main")

    val topic = "simple-topic"

    // Map을 써도 됨.
    val props = Properties();

    //bootstrap server, key / value serializer class

    //single broker
//    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);

    val kafkaProducer: KafkaProducer<String, String> = KafkaProducer<String, String>(props)

    //key가 없기 때문에 partitioner 전략에 따름(2.4 이후부터는 sticky)
    val producerRecord: ProducerRecord<String, String> = ProducerRecord(topic, "hi")


    kafkaProducer.send(producerRecord) { recordMetadata, exception ->

        exception?.let{
            logger.error("exception error from broker {}", it.message)
            return@send
        }

        logger.info("### record metadata received ###")
        logger.info("partition : {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp())
    }

    Thread.sleep(3000)

    kafkaProducer.flush()
    kafkaProducer.close()
}