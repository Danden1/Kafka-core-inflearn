# Kafka 실습

강의와 달리 kotlin 을 이용해서 진행

## Producer

`KafkaProducer`의 `send()` 를 이용하면 비동기로 데이터를 보내게 됨.

> main thread에서 send()를 호출 후, 별도의 thread에서 serializer -> partitioner -> sender 순으로 전송됨.


이 때, 바로 데이터를 보내지 않고 설정한 batch size나 linger.ms에 따라서 데이터를 보내게 됨(네트워크 오버헤드 줄이기 위해)

`send()` 메소드의 return type은 future임. `.get()`을 호출하여 sync로 처리 가능함.

-> 이래서 MSA에서 주로 이용한다고 느낄 수 있었음. 사실 tcp 응답까지 기다리는 줄 알았는데 아니였음.

-> 그렇다면, retry 등 이런 전략은 어떻게 되는지?

-> 회사 로깅 관련해서 redis stream을 이용해서 했을 때, 따로 비동기 설정은 하지 않았었음. 이 부분 고려해보면 좋을 것으로 보임.

<br>

### 비동기 관련 설명

비동기로 동작할 때, ack을 받기 위해서는 callback 을 이용함. 

callback은 다른 함수의 인자로서 전달된 후에, 특정 이벤트 발생 시 해당 함수에서 다시 호출됨. (kafka 뿐만이 아니라 여러 곳에서 callback 사용함)

-> spring webflux도 이용하지 않나?

-> reactor 를 이용해서 처리하기 때문에, 개발자가 실제적으로 callback을 사용하지는 않음(콜백헬 문제 피할 수 있음. 내부에서는 콜백 사용함)

```java
loadUser(id, user -> {
    loadPosts(user, posts -> {
        sendNotification(posts, result -> {
            System.out.println("done!");
        });
    });
});

// 이처럼 사용가능
findUser(id)
    .flatMap { user -> findPosts(user) }
    .flatMap { posts -> sendNotification(posts) }
    .subscribe { println("done!") }
```

여기서 retry 관련된 이야가 나옴.

ack을 받아서 보고 처리를 해야함. 이는 callback을 이용하기 때문임.

카프카 내부에서 `acks`가 `0`이면 재전송을 하지 않음. `1`이나 `-1` 이면 함.


```kotlin

kafkaProducer.send(producerRecord) { recordMetadata, exception ->

    exception?.let{
        logger.error("exception error from broker {}", it.message)
        return@send
    }

    logger.info("### record metadata received ###")
    logger.info("partition : {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp())
}
```

를 이용할 경우, 결과가

> [kafka-producer-network-thread | producer-1] INFO main - ### record metadata received ###
[kafka-producer-network-thread | producer-1] INFO main - partition : 0, offset: 17, timestamp: 1748786630363


처럼 나옴. 즉, 다른 thread에서 callback이 호출됨.


### ack 설정

멀티 broker 환경에서, leader에 메시지를 보내고 follower에 데이터를 복사함.

`acks = 0` 이면, ack을 받지 않고 바로 보냄. 데이터 손실이 되도 큰 상관이 없는 경우 사용

`acks = 1` leader에게 데이터를 보내면, leader가 write에 관한 ack 만 봄. 즉, follower에 데이터가 복사되었는 지는 모름. 만약 이 과정에서 leader에 문제가 생기면 데이터 유실이 될 수 있음.(follower의 데이터는 업데이트 되지 않음.)

`acks = -1(all)`(default?) 이면, leader가 `min.insync.replicas` 개수 만큼의 replicator 에 데이터가 복사되면 ack을 받음. 2로 설정되어 있다면, 3대 중 2대가 죽으면 에러 발생함. 즉, 데이터 손실이 발생하지 않지만 전송 속도가 느림.


callback 기반의 async에서 acks 설정에 기반하여 retry 수행됨.

sync 방식에서 acks = 0 일 경우, ack을 기다리지 않음.(fire and forget, 거의 이렇게 사용하지 않음.. ack이 중요하지 않기 때문에 그냥 던지고 다른 일을 하면 되는데 sync로 기다릴 필요가 없음.)


`acks` 0으로 보내면,

> [main] INFO main - sync message : Q001, partition : 1, offset: -1

처럼 offset 정보를 받지 못함.


### 배치 전송

Serialize -> Partitioning -> Compression(선택) -> record accumulator 저장(배치 단위) -> Sender에서 별도의 Thread로 전송

`send()` 를 호출해도 바로 전송되지 않고 내부 메모리에 저장됨.

`buffer.memory` 설정 사이즈 만큼 데이터 보관될 수 있음. 이는 전체 메모리 사이즈임.

`batch.size` 는 해당 partition 의 단일 배치 사이즈를 의미.

`linger.ms` 는 최대 대기 시간임. batch size만큼 차지 않아도 해당 시간이 경과하면 데이터 보냄.

#### Producer의 Sync, callback async

`Callback` 기반의 Async는 여러 개의 메시지가 batch로 만들어짐.

.get() 을 이용하여 sync 방식을 이용하면, 메시지 베치 처리가 불가능함. 전송 자체는 배치 레벨로 되지만, 배치에 메시지는 1개만 있음.
(응답을 받기 위해 block이 됨)



