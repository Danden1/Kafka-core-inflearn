package com.exmaple.kafka

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception

class CustomCallback(private val seq: Int) : Callback {

    private val logger: Logger = LoggerFactory.getLogger(CustomCallback::class.java)
    override fun onCompletion(p0: RecordMetadata?, p1: Exception?) {
        p1?.let {
            logger.error("exception error from broker {}", it.message)
            return
        }

        logger.info("### record metadata received ###")
        logger.info("seq : {}, partition : {}, offset: {}, timestamp: {}", seq, p0!!.partition(), p0.offset(), p0.timestamp())
    }
}
