package com.exmaple.kafka

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils
import org.slf4j.LoggerFactory


private val logger = LoggerFactory.getLogger("main")

class CustomPartitioner : Partitioner {


    private var specialKeyName = ""

    //ProducerConfig에 들어가는 값.
    override fun configure(configs: MutableMap<String, *>?) {
        specialKeyName = configs?.get("custom.specialKey").toString()
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun partition(topic: String?, key: Any?, keyBytes: ByteArray?, value: Any?, valueBytes: ByteArray?, cluster: Cluster?): Int {
        val partitionInfoList : List<PartitionInfo> =cluster!!.partitionsForTopic(topic)
        val numPartition : Int = partitionInfoList.size
        val numSpecialPartitions : Int = numPartition / 2

        var partitionIndex = -1



        key?.let {
            if (key is String && key == specialKeyName) {

                partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions
            }
        }

        if (partitionIndex == -1) {
            partitionIndex =  Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartition - numSpecialPartitions) + numSpecialPartitions
        }

        logger.info("key : {} is sent to partition : {}", key, partitionIndex)

        return partitionIndex
    }
}