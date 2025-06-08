package com.exmaple.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class ConsumerWakeUp {



    companion object {

        private val logger : Logger = LoggerFactory.getLogger(ConsumerWakeUp::class.java)
        @JvmStatic
        fun main(args: Array<String>) {

            val topic = "simple-topic"

            val props = Properties()
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01")

            val kafkaConsumer : KafkaConsumer<String, String> = KafkaConsumer<String, String>(props)
            kafkaConsumer.subscribe(listOf(topic))

            val mainThread = Thread.currentThread()

            Runtime.getRuntime().addShutdownHook(
                // 익명 클래스
                Thread {
                    logger.info("Shutdown hook triggered, waking up the consumer...")
                    kafkaConsumer.wakeup() // This will throw a WakeupException

                    mainThread.join()
                }

            )

            try {
                while (true) {
                    val records: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(1000))

                    for (record: ConsumerRecord<String, String> in records) {
                        logger.info(
                            "record key: {}, value: {}, partition: {}, offset: {}, timestamp: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset(),
                            record.timestamp()
                        )
                    }
                }
            } catch (e: WakeupException) {
                logger.error("WakeupException caught: {}", e.message)
            } finally {
                kafkaConsumer.close()
                logger.info("Kafka consumer closed gracefully.")
            }


        }
    }

}