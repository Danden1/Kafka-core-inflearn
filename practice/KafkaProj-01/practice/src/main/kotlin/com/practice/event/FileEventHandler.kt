package com.practice.event

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

class FileEventHandler(
    private val kafkaProducer: KafkaProducer<String, String>,
    private val topic: String,
    private val sync: Boolean
) : EventHandler {

    companion object {
        private val logger = LoggerFactory.getLogger(FileEventHandler::class.java)
    }

    override fun onMessage(messageEvent: MessageEvent) {
        val producerRecord: ProducerRecord<String, String> = ProducerRecord(
            topic,
            messageEvent.key,
            messageEvent.value
        )

        if (sync) {
            val future = kafkaProducer.send(producerRecord)
            val recordMetadata = future.get()

            logger.info("### record metadata received ###")
            logger.info(
                "partition : {}, offset: {}, timestamp: {}",
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp()
            )
        } else {
            kafkaProducer.send(producerRecord) { recordMetadata, exception ->
                exception?.let {
                    logger.error("exception error from broker {}", it.message)
                    return@send
                }

                logger.info("### record metadata received ###")
                logger.info(
                    "partition : {}, offset: {}, timestamp: {}",
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    recordMetadata.timestamp()
                )
            }
        }
    }
}