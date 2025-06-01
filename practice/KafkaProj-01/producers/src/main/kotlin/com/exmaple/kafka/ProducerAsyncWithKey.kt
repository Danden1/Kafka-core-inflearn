package org.example.com.exmaple.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

fun main() {
    val logger = LoggerFactory.getLogger("main")

    val topic = "multipart-topic"

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
    //결과 출력하면, 하나의 partition 의 offset 5까지 쌓인 후 다른 partition 에 쌓이는 것을 볼 수 있음.
    for (i in 0..19) {
        val producerRecord: ProducerRecord<String, String> = ProducerRecord(topic, "$i" ,"hi $i")

        //비동기로 수행이 됨. 내가 보낸 메시지가 어느 partition 에 들어가는 지 보려면 어떻게 해야하는 지?
        kafkaProducer.send(producerRecord) { recordMetadata, exception ->

            exception?.let {
                logger.error("exception error from broker {}", it.message)
                return@send
            }

            logger.info("### record metadata received ###")
            logger.info("partition : {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp())
        }
    }


    Thread.sleep(3000)

    kafkaProducer.flush()
    kafkaProducer.close()
}