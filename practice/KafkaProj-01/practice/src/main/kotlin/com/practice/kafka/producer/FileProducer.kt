package com.practice.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.util.Properties

class FileProducer {

    companion object {

        private val logger = LoggerFactory.getLogger(FileProducer::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            val logger = LoggerFactory.getLogger("main")

            val topic = "file-topic"

            // Map을 써도 됨.
            val props = Properties();

            //bootstrap server, key / value serializer class

            //single broker
//    props.setProperty("bootstrap.servers", "localhost:9092");
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);
            props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);

            val kafkaProducer: KafkaProducer<String, String> = KafkaProducer<String, String>(props)

            val filePath =
                "/Users/yongha/develop/study/kafka/Kafka-core-inflearn/practice/KafkaProj-01/practice/src/main/resources/pizza_sample.txt"

            sendFileMessage(kafkaProducer, topic, filePath)



            kafkaProducer.close()
        }

        private fun sendFileMessage(kafkaProducer: KafkaProducer<String, String>, topic: String, filePath: String) {
            val delimiter = ","

            try {
                val fileReader: FileReader = FileReader(filePath)
                val bufferedReader: BufferedReader = BufferedReader(fileReader)
                var line: String? = null

                File(filePath).forEachLine { line ->
                    val tokens = line.split(delimiter)
                    val key = tokens[0]

                    val value: StringBuffer = StringBuffer()

                    for (i in 1 until tokens.size) {
                        value.append(tokens[i])
                        if (i < tokens.size - 1) {

                            if (i != (tokens.size - 1)) {
                                value.append(tokens[i] + ",")
                                continue
                            }

                            value.append(tokens[i])

                        }
                    }

                    sendMessage(kafkaProducer, topic, key, value.toString())
                }


            } catch (e: IOException) {
                logger.info(e.message)
            }
        }

        private fun sendMessage(
            kafkaProducer: KafkaProducer<String, String>,
            topic: String,
            key: String,
            value: String
        ) {

            val producerRecord: ProducerRecord<String, String> = ProducerRecord(topic, key, value)

            logger.info("key: {}, value: {}", key, value)

            kafkaProducer.send(producerRecord) { recordMetadata, exception ->

                exception?.let {
                    logger.error("exception error from broker {}", it.message)
                    return@send
                }

                logger.info("### record metadata received ###")
                logger.info("partition : {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp())
            }
        }


    }
}