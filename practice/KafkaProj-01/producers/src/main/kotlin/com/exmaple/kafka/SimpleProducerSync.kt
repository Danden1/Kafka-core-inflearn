package org.example.com.exmaple.kafka

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

    val kafkaProducer : KafkaProducer<String, String> = KafkaProducer<String, String>(props)

    //key가 없기 때문에 partitioner 전략에 따름(2.4 이후부터는 sticky)
    val producerRecord : ProducerRecord<String, String> = ProducerRecord(topic, "hi")


    try {
        val future = kafkaProducer.send(producerRecord)
        val recordMetadata = future.get()

        logger.info("### record metadata received ###")
        logger.info("partition : {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp())
    } catch (e: Exception) {
        e.printStackTrace()
    }

    kafkaProducer.flush()
    //close 시, flush도 자동으로 함. batch로 동작하기 때문에 flush가 필요함
    kafkaProducer.close()

}