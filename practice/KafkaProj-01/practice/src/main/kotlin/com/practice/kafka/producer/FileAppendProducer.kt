package com.practice.kafka.producer

import com.practice.event.EventHandler
import com.practice.event.FileEventHandler
import com.practice.event.FileEventSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.util.*

class FileAppendProducer {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
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
            val file =
                File("/Users/yongha/develop/study/kafka/Kafka-core-inflearn/practice/KafkaProj-01/practice/src/main/resources/pizza_append.txt")
            val eventHandler: EventHandler = FileEventHandler(kafkaProducer, topic, true)

            val fileEventSource: FileEventSource = FileEventSource(10000, file, eventHandler)

            val fileEventThread : Thread = Thread(fileEventSource)
            fileEventThread.start()

            fileEventThread.join()
        }
    }
}