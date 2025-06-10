package com.exmaple.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class SimpleConsumer {



    companion object {

        val logger : Logger = LoggerFactory.getLogger(SimpleConsumer::class.java)
        @JvmStatic
        fun main(args: Array<String>) {

            val topic = "simple-topic"

            val props = Properties()
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group")

            //heart beat thread 설정
            props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000")
            props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000")
            props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000")

            val kafkaConsumer : KafkaConsumer<String, String> = KafkaConsumer<String, String>(props)
            kafkaConsumer.subscribe(listOf(topic))

            while (true) {
               val records : ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(1000))

               for (record : ConsumerRecord<String, String> in records) {
                    logger.info("record key: {}, value: {}, partition: {}, offset: {}, timestamp: {}",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset(),
                        record.timestamp())
                }
            }


        }
    }

}