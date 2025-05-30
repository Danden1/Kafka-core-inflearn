package com.exmaple.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class SimpleProducer {


}

fun main() {

    val topic : String = "simple-topic"

    // Map을 써도 됨.
    val props = Properties();

    //bootstrap server, key / value serializer class

    //single broker
//    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name);

    val kafkaProducer : KafkaProducer<String, String> = KafkaProducer<String, String>(props)

    //key가 없기 때문에 partitioner 전략에 따름(2.4 이후부터는 sticky)
    val producerRecord : ProducerRecord<String, String> = ProducerRecord(topic, "hi")

    //future로 보냄. 별도의 thread가 전송을 담당함 -> 비동기
    //send() ->  | Serializer -> Partitioner -> sender
    //send() 이후에는 별도의 thread에서 동작. 또한 buffer 모은 후에, 한 번에 보냄. 한 건 씩 보내면 네트워크 오버헤드 발생함.
    //default thread 개수는 몇 개인지?
    kafkaProducer.send(producerRecord)


    kafkaProducer.flush()
    //close 시, flush도 자동으로 함. batch로 동작하기 때문에 flush가 필요함
    kafkaProducer.close()

}