package com.exmaple.kafka

import com.github.javafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

val logger: Logger = LoggerFactory.getLogger("main")
fun sendPizzaMessages(
    kafkaProducer: KafkaProducer<String, String>,
    topicName : String,
    iterCount : Int,
    interIntervalMs : Int, intervalMs : Int, intervalCount: Int, sync : Boolean
    ) {

    val pizzaMessage = PizzaMessage()

    var iterSeq = 0;
    val seed: Long = 2022
    val random = Random(seed)
    val faker = Faker.instance(random)

    while(iterSeq != iterCount) {
        val message = pizzaMessage.produceMsg(faker, random, iterSeq)
        val produceRecord = ProducerRecord<String,String>(topicName, message["key"], message["message"])

        sendMessage(kafkaProducer, produceRecord, message, sync)

        if (intervalCount > 0 && iterSeq % intervalCount == 0) {
            logger.info("### intervalCount : {}, intervalMs : {}", intervalCount, intervalMs)
            Thread.sleep(intervalMs.toLong())
        }

        if(interIntervalMs > 0) {
            logger.info("### interIntervalMs : {}", interIntervalMs)
            Thread.sleep(interIntervalMs.toLong())
        }

        iterSeq++

    }
}


fun sendMessage(kafkaProducer: KafkaProducer<String, String>, producerRecord : ProducerRecord<String, String>, message: HashMap<String, String>, sync: Boolean) {

    if (!sync) {

        kafkaProducer.send(producerRecord) { recordMetadata, exception ->

            exception?.let {
                logger.error("exception error from broker {}", it.message)
                return@send
            }

            logger.info("### record metadata received ###")
            logger.info(
                "async message : {}, partition : {}, offset: {}",
                message["key"],
                recordMetadata.partition(),
                recordMetadata.offset(),
            )
        }
    }
    else {
        val recordMetadata = kafkaProducer.send(producerRecord).get()

        logger.info(
            "sync message : {}, partition : {}, offset: {}",
            message["key"],
            recordMetadata.partition(),
            recordMetadata.offset(),
        )
    }
}

fun main() {


    val topic = "pizza-topic"

    // Map을 써도 됨.
    val props = Properties();

    //bootstrap server, key / value serializer class

    //single broker
//    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);

    val kafkaProducer: KafkaProducer<String, String> = KafkaProducer<String, String>(props)

    sendPizzaMessages(kafkaProducer, topic, -1, 10, 100, 100, true)

    kafkaProducer.close()
}