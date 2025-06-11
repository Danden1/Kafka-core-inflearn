package com.example.kafka

import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.List

class ConsumerCommit {
    val logger: Logger = LoggerFactory.getLogger(ConsumerCommit::class.java.name)

    companion object fun main(args: Array<String>) {
        val topicName = "pizza-topic"

        val props = Properties()
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_03")
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        props.setProperty(
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            CooperativeStickyAssignor::class.java.name
        )

        val kafkaConsumer = KafkaConsumer<String, String>(props)
        kafkaConsumer.subscribe(listOf(topicName))

        //main thread
        val mainThread = Thread.currentThread()

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info(" main program starts to exit by calling wakeup")
                kafkaConsumer.wakeup()

                try {
                    mainThread.join()
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            }
        })

        //kafkaConsumer.close();
        //pollAutoCommit(kafkaConsumer);
        //pollCommitSync(kafkaConsumer);
        pollCommitAsync(kafkaConsumer)
    }

    private fun pollCommitAsync(kafkaConsumer: KafkaConsumer<String, String>) {
        var loopCnt = 0

        try {
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000))
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count())
                for (record in consumerRecords) {
                    logger.info(
                        "record key:{},  partition:{}, record offset:{} record value:{}",
                        record.key(), record.partition(), record.offset(), record.value()
                    )
                }
                kafkaConsumer.commitAsync { offsets, exception ->
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.message)
                    }
                }
            }
        } catch (e: WakeupException) {
            logger.error("wakeup exception has been called")
        } catch (e: Exception) {
            logger.error(e.message)
        } finally {
            logger.info("##### commit sync before closing")
            kafkaConsumer.commitSync()
            logger.info("finally consumer is closing")
            kafkaConsumer.close()
        }
    }

    private fun pollCommitSync(kafkaConsumer: KafkaConsumer<String, String>) {
        var loopCnt = 0

        try {
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000))
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count())
                for (record in consumerRecords) {
                    logger.info(
                        "record key:{},  partition:{}, record offset:{} record value:{}",
                        record.key(), record.partition(), record.offset(), record.value()
                    )
                }
                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync()
                        logger.info("commit sync has been called")
                    }
                } catch (e: CommitFailedException) {
                    logger.error(e.message)
                }
            }
        } catch (e: WakeupException) {
            logger.error("wakeup exception has been called")
        } catch (e: Exception) {
            logger.error(e.message)
        } finally {
            logger.info("finally consumer is closing")
            kafkaConsumer.close()
        }
    }


    fun pollAutoCommit(kafkaConsumer: KafkaConsumer<String?, String?>) {
        var loopCnt = 0

        try {
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000))
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count())
                for (record in consumerRecords) {
                    logger.info(
                        "record key:{},  partition:{}, record offset:{} record value:{}",
                        record.key(), record.partition(), record.offset(), record.value()
                    )
                }
                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000)
                    Thread.sleep(10000)
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            }
        } catch (e: WakeupException) {
            logger.error("wakeup exception has been called")
        } finally {
            logger.info("finally consumer is closing")
            kafkaConsumer.close()
        }
    }
}