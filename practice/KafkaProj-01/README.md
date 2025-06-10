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

`acks = -1(all)`(default) 이면, leader가 `min.insync.replicas` 개수 만큼의 replicator 에 데이터가 복사되면 ack을 받음. 2로 설정되어 있다면, 3대 중 2대가 죽으면 에러 발생함. 즉, 데이터 손실이 발생하지 않지만 전송 속도가 느림.


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


### 메시지 재전송

> `deliver.timeout.ms` >= linger.ms + request.timeout.ms

만약 이 조건을 만족하지 않으면, 에러 발생하고 실행 자체가 안됨.

linger.ms가 포함되어 있는 이유는 send() 호출 후, 걸리는 총 시간이기 때문임?

acks = 1 or all 이면,

만약 Record Accumulator가 전부 차있고, sender thread에서는 데이터 전송이 오래 걸리면 send()를 해도 기다리는 경우가 있음.

- `max.blocks.ms` 만큼 기다림. 초과 시, Timeout Exception. default 60_000ms
- `linger.ms` 만큼 최대 대기 시간
- sender thread 가 데이터를 보내고 `request.timeout.ms` 만큼 기다림. 초과 시, retry 하거나 time out excepetion 발생. default 30_000ms
- `retry.backoff.ms` 만큼 기다림. default 100ms
- `deliver.timeout.ms` 메시지 전송에 허용된 **최대 시간**. 초과 시 timeout exception 발생. default 120_000ms

<br>

`max.in.flight.requests.per.connection` 은 `브로커 서버 응답` 없이 producer의 sender thread가 보낼 수 있는 최대 요청 수임. default 5.

kafka producer의 메시지 전송 단위는 batch 임.

비동기 전송 시 브로커의 응답없이 한 번에 보낼 수 있는 batch의 개수.

<br>

그러면 파티션 개수 * `max.in.flight.requests.per.connection` 만큼의 메시지를 보낼 수 있음

단, 2 이상 값으로 설정되어 있다면 순서 보장이 되지 않을 수 있음. b1, b0 를 보내고, b0가 실패하면 b1이 먼저 적재되고 b0는 retry 로직 등을 통해 수행될 수 있음.

이를 해결하기 위해서 `enable.idempotence=true` 를 통해 어느 정도 해결할 수 있음.(멱등성)


### 최대 한번(at most once), 최소 한번(at least once), 정확히 한번(exactly once)

Transaction 기반 전송. Consumer -> Process -> Producer (주로 kafka stream 에서)에 주로 사용되는 트랜잭션 기반 처리

- at most once : 메시지 전송 시, ack을 받지 않고 바로 전송함. 데이터 유실이 발생할 수 있음. 중복 전송은 하지 않음.
- at least once : 메시지 전송 시, ack을 받고 전송함. 데이터 유실은 발생하지 않지만, 중복 전송이 발생할 수 있음. 실제 적재는 되었지만, ack을 받지 못하면 pub을 retry를 함으로써 데이터 중복 저장이 발생할 수 있음.
- exactly once : 메시지 전송 시, ack을 받고 전송함. 데이터 유실도 발생하지 않고, 중복 전송도 발생하지 않음. 트랜잭션 기반 처리로 구현됨.

#### 멱등성(idempotence)

`producer id`, `message sequence` 를 header 에 저장하여 전송함.

브로커에서 message sequence가 중복이면, 메시지 로그에 기록하지 않고 ack 만 전송함.

브로커는 producer 가 보낸 메시지의 sequence 가 브로커가 가지고 있는 메시지의 `sequence 보다 1만큼 큰 경우`에만 브로커에 저장함.

이를 이용하기 위해서는

```
# 이 옵션을 제외하고 다른 파라미터들을 잘못 설정하면, acks =1 등 정상적으로 메시지는 보내지만, 멱등성이 보장되지 않을 수 있음!)
enalbe.idempotence=true
acks=all
retries > 0

max.in.flight.requests.per.connection=1 ~ 5 # 6 이상은 안됨. 
# 메시지는 전송이 되지만, 멱등성 보장이 되지 않음! 

```

> if `enable.idempotence` is set to true, ordering will be preserved. Additionally, enabling idempotence requires the value of this configuration to be **less than or equal to 5**. If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled.

출처 : https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#max-in-flight-requests-per-connection

멱등성을 키면 성능이 약간 감소(최대 20%) 하지만, 기본적으로 설정하는 것을 권장함! (3.0 부터는 기본 설정임.)

max.in.flight.requests.per.connection 만큼 여러 개의 배치들이 broker에 전송이 됨.

b2, b1, b0 -> 을 보낸다고 하면, b0은 저장 성공! b1은 저장 실패! 하면, b2의 seq 번호가 첫 메시지의 seq + 2 이기 때문에 out of sequence 에러가 발생함.

-> 그렇다면 실제 어플리케이션에선 어떻게 수정을 해야 하는 지? -> 분산 트랜잭션 등 필요해보임!

### custom partitioner

default로 key값이 없다면 파티셔너 전략에 따름(앞 부분 참고)

만약 key가 있다면, murmur2 알고리즘으로 해시 값을 구한 후 partition 개수로 나눈 나머지 값을 partition 으로 사용함.


## Consumer

`consumer`는 `poll()` 메소드를 이용하여 주기적으로 브로커의 토픽 파티션에서 데이터를 가져옴.

성공적으로 가져왔으면, `commit`을 통해 __consumer_offsets 을 갱신함.

heart beat thread를 통해 Group Coordinator에 보고하는 역할을 함.

만약 heart beat를 받지 못하면, rebalance 수행.

producer에서 `serilializer`를 통해 직렬화된 데이터를 가져오고, `deserializer`를 통해 역직렬화하여 사용함.

`group.id` 를 설졍해야 함.

cosumer는 `close()` 하는 것이 매우 중요함!

안하면,

> [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_01-1, groupId=group_01] (Re-)joining group
> [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_01-1, groupId=group_01] Request joining group due to: need to re-join with the given member-id: consumer-group_01-1-d450100c-dc89-4263-8a74-18cad1f1b702
> [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_01-1, groupId=group_01] (Re-)joining group

같은 메시지가 뜨면서 연결하는 데 매우 오래 걸림.

그 이유는 group coordinator 가 해당 consumer 가 살아있다고 판단해서?

### poll

Consumer 안에는

Fetcher, ConsumerNetworkClient, SubscriptionState, ConsumerCoordinator, HeartbeatThread 등이 있음.

- 브로커나 Consumer 내부 큐에 데이터가 있다면, 바로 데이터를 반환.
- 그렇지 않은 경우에는 Duration 동안 데이터 Fetch를 브로커에 계속 수행

첫 번째 poll() 때, meta data 을 가져오는 등 다양한 작업을 한다고 함.

```kotlin

kafkaConsumer.poll(Duration.ofMillis(1000)).forEach { record ->
    logger.info("### record received ###")
    logger.info("key : {}, value : {}, partition : {}, offset: {}", record.key(), record.value(), record.partition(), record.offset())
    // RDB, NoSQL 등 데이터 작업
}
```

`Fetcher`는 데이터를  LinkedQueue 에서 가져오고, poll() 이 완료됨.

`ConsumerNetworkClient` 가 비동기로 kafka에서 데이터를 가져온 후, Linked Queue 에 넣어줌.

Linked Queue 에 데이터가 없을 경우 duration 동안 broker에 미세지 요청 후, poll 수행 완료.

만약 데이터가 중간에 들어오면, duration 동안 기다리지 않고 바로 데이터를 가져오고 queue 에 저장함.


#### Fetcher

설정

- `fetch.min.bytes` (default : 1) : recorde를 읽어들이는 최소 byte. 이상의 새로운 메시지가 쌓일때 까지 kafka로부터 데이터를 가져오지 않음.(ConsumerNetworkClient 설정)
- `fetch.max.wait.ms`(default : 500) : fetcher가 데이터를 가져오기 위해 기다리는 최대 시간. 이 시간이 지나면, 최소 바이트보다 적더라도 데이터를 가져옴.
- `fetch.max.bytes`(default : 52_428_800) : fetcher가 가져올 수 있는 최대 바이트.
- `max.partition.fetch.bytes`(default : 1_000_000) : fetcher가 가져올 수 있는 최대 파티션당 바이트.
- `max.poll.records`(default : 500) : poll() 메소드가 가져올 수 있는 최대 레코드 수. 실제 fetcher 가 가져오는 데이터 개수

<br>

poll 실행 시, 일어나는 일

1. 가져올 데이터가 1 건도 없으면 poll()의 duration 만큼 대기 후 return
2. 가져와야할 데이터가 많다면, `max.partition.fetch.bytes` 배치 크기 설정. 그렇지 않은 경우, `fetch.min.bytes`로 배치 크기 설정? -> 강사 분 뇌피셜이라고 함. ??
3. 가장 최신의 offset 데이털르 가조오고 있다면, `fetch.min.bytes` 만큼 가져와서 return 함. `fetch.min.bytes` 만큼 쌓이지 않았다면 `fetch.max.wait.ms` 만큼 기다린 후, 데이터를 가져옴.
4. 오랜 과거 offset 데이터를 가져온다면, `max.partition.fetch.bytes` 만큼 파티션에서 데이터를 읽은 뒤 반환.
5. `max.partition.fetch.bytes` 에 도달하지 못하여도 가장 최신의 offset에 도달하면 반환
6. 토픽에 파티션이 많아도 가져오는 데이터량은 `fetch.max.bytes` 로 제한됨.
7. Fetcher가 LinkedQueue에서 가져오는 데이터 개수는 `max.poll.records` 로 제한됨.



### subscribe, poll, commit 로직

subscribe() 를 통해 topic 구독함.

poll() 메소드를 이용하여 메시지를 주기적으로 읽어올 수 있음.

메시지를 성공적으로 가져왔으면 commit을 통해 __consumer_offsets(broker 내부에 있음) 을 갱신함


### offset

consumer가 topic에 `처음` 접속하여 message를 가져올 때, 어디서부터 가져올 것인지 설정 가능.

- `auto.offset.reset` : consumer가 처음 topic에 접속했을 때, 어디서부터 가져올 것인지 설정함. `earliest`(default) 는 가장 오래된 메시지부터 가져오고, `latest` 는 가장 최신 메시지부터 가져옴.

`__consumer_offsets` 은 consumer group 별로 관리가 됨.

- `offset.retention.minutes` : 해당 consumer group의 offset을 얼마나 유지할 것인지 설정함. default 7일


### rebalancing

consumer group에 새로운 consumer가 추가되거나, 기존 consumer가 제거될 때 발생함.

broker의 group coordinator 가 진행하게 됨.

1. consumer group 내의 consumer가 broker에 최초 접속 요청 시, group coordinator 가 새성됨.
2. 동일 group.id로 여러 개의 consumer로 broker의 group coordinator 에 접속
3. 가장 빨리 요청한 consumer가 leader consumer(group 내의)로 지정됨.
4. consumer leader 는 파티션 할당 전략에 따라 consumer 들에게 파티션 정보를 할당함.(leader 가 죽은 지 감지하려면?)
5. leader consumer 는 최종 할당된 파티션 정보를 group coordinator 에 전달함.
6. consumer 들이 정보 읽음

consumer 가 죽었는 지 판단하기 위해 heart beat를 주기적으로 보냄. -> 죽으면, group coordinator 가 리밸런싱하라고 전달함.

`GroupMetaData`로 그룹 정보를 관리함

consumer 가 없으면 `empty` 상태. consumer 추가 되면, 리밸런싱 진행후 `stable` 상태로 변경.
만약 다시 없어지면 `empty` 상태로 변경됨.


### static group membership

consumer 들이 많아지고, 리밸런싱이 일어나면 많은 일이 일어날 수 있음.

consumer restart도 리밸런싱이 발생함 -> 이 경우 불필요한 리밸런싱이 일어나게 됨.

-> 이를 방지하기 위해 수행.

consumer 들에게 고정된 id를 부여.

consumer 가 다운 되어도, `seesion.timeout.ms` 내 에 재기동 되면, rebalancing 발생하지 않음.


### heart beat thread

heart beat thread 를 통해 브로커의 group coordinator 에 주기적으로 heart beat(consumer 상태 확인)를 보냄.

consumer parameter(broker 파라미터 아님)

- `heartbeat.interval.ms` (default : 3_000) : 만큼 주기적으로 heart beat 를 보냄. `session.timeout.ms` 보다 작아야 함. `session.timeout.ms` 보다 1/3 보다 낮게 설정 권장
- `session.timeout.ms` (default : 45_000) : 브로커가 heart beat를 기다리는 최대 시간. 이 시간이 지나면, 리밸런싱이 발생함.
- `max.poll.interval.ms` (default : 30_000) : poll() 메소드가 호출되지 않는 최대 시간. 이 시간이 지나면, 리밸런싱이 발생함. 즉, poll() 메소드를 호출하지 않으면, 리밸런싱이 발생함. 예를 들면, poll 하고 다른 작업(rdbms 등)에 넣는 시간이 오래 걸리면, 리밸런싱이 일어날 수 있음!!!!

`max.poll.interval.ms`를 잘 설정해야 함!

해당 시간 안에 모든 작업 처리할 수 있도록 옵션 조절 필요! 설정하지 않으면 의도치 않은 리밸런싱이 일어나게 됨.

```
[kafka-coordinator-heartbeat-thread | group_02] WARN org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_02-1, groupId=group_02] consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time processing messages. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.
[kafka-coordinator-heartbeat-thread | group_02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_02-1, groupId=group_02] Member consumer-group_02-1-c9d0c55a-a5ac-41c9-b31a-e76547004cb4 sending LeaveGroup request to coordinator localhost:9092 (id: 2147483646 rack: null) due to consumer poll timeout has expired.
[kafka-coordinator-heartbeat-thread | group_02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_02-1, groupId=group_02] Resetting generation and member id due to: consumer pro-actively leaving the group
[kafka-coordinator-heartbeat-thread | group_02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group_02-1, groupId=group_02] Request joining group due to: consumer pro-actively leaving the group
```
위와 같은 메시지를 마주치게 되고, 다음 poll() 호출할 때는 다시 meta 정보 부터 가져옴(consumer의 첫 poll 요청은 실제 record를 가져오지 않음. meta 정보 가져옴)


### rebalance mode

#### Eager

기본으로 사용하고 있는 모드.

rebalacing이 발생하면, 기존 consumer 들의 모든 파티션 할당 취소하고 잠시 메시지 읽지 않음.

파티션 할당 전략(range, round robing, sticky)에 따라 partition 할당 후 메시지 읽음.

partition 이 많아지면, 대용량 메시지 처리를 하지 못하게 됨.


#### (Incremental) Cooperative

Eager랑 다르게 모든 파티션 할당 취소하지 않음.

대상이 되는 consumer들에 대해서 파티션에 따라 rebalancing이 점진적으로 일어남.

### partition 할당 전략

- Consumer 의 부하를 파티션 별로 균등하게 할당해야 함.

- 리밸런싱 및 데이터 처리 효율성 극대화 가능

<br>

**할당 전략**

- Range : 서로 다른 2개 이상의 토픽을 구독할 시, **토픽 별로 동일한 파티션**을 특정한 consumer 에 할당. -> key를 동일한 키 처리 가능(e.g order id 등). 하지만 특정 consumer에 부하가 몰릴 수 있음.
- Round robin : partition 을 순차적으로 consumer 에 할당. rebalance 일어나면, 모든 consumer에 대해 round robin 으로 다시 파티션 할당해주어야 함! 즉. consumer가 읽던 partition 정보가 크게 바뀌게 됨.(사라질 수 있음)
- sticky : 최초 할당된 파티션과 consumer 매핑을 rebalance 수행되어도 가급적으로 그대로 유지할 수 있도록 지원(eager). sticky 는 RR과 달리 기존에 consumer가 구독하고 있던 파티션은 계속 읽음. 
- Cooperative sticky : rabalance 시 모든 consumer의 파티션 매핑이 해제되지 않고 rebalance 연관된 파티션과 consumer 만 재매핑됨(eager이 일어나지 않음)

<br>

기본인 Range 기반으로 하고 consumer 2개, topic 2개, partition 각 3개로 설정 하면,

아래처럼 같은 파티션에 대해서 읽고 있음.
```
partitions=[topic-p3-t2-0, topic-p3-t2-1, topic-p3-t1-0, topic-p3-t1-1])
Notifying assignor about the new Assignment(partitions=[topic-p3-t2-2, topic-p3-t1-2])
```

partition 의 개수보다 많은 4대로 하면, consumer 하나는 아무 토픽도 구독하고 있지 않음! -> 이 부분은 강의에 없음.

```
Notifying assignor about the new Assignment(partitions=[])
```

반면에 Round robin 으로 consumer 4대를 기동시키면, 각 consumer에 partition 개수 2/2/1/1 로 할당됨



Cooperative sticky로 하면, 앞의 3개와 달리

```
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-mtopic-1, groupId=group-mtopic] Updating assignment with
	Assigned partitions:                       [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t1-2, topic-p3-t2-0, topic-p3-t2-1, topic-p3-t2-2]
	Current owned partitions:                  []
	Added partitions (assigned - owned):       [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t1-2, topic-p3-t2-0, topic-p3-t2-1, topic-p3-t2-2]
	Revoked partitions (owned - assigned):     []
```
처럼 보이게 됨!

consumer 가 1개 더 추가 되면, 기존 consumer의 로그에서

```
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-mtopic-1, groupId=group-mtopic] Updating assignment with
	Assigned partitions:                       [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t2-0]
	Current owned partitions:                  [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t1-2, topic-p3-t2-0, topic-p3-t2-1, topic-p3-t2-2]
	Added partitions (assigned - owned):       []
	Revoked partitions (owned - assigned):     [topic-p3-t1-2, topic-p3-t2-1, topic-p3-t2-2] //포기하는 partition
```

가 나온 후에, 

```
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-mtopic-1, groupId=group-mtopic] Updating assignment with
	Assigned partitions:                       [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t2-0]
	Current owned partitions:                  [topic-p3-t1-0, topic-p3-t1-1, topic-p3-t2-0]
	Added partitions (assigned - owned):       []
	Revoked partitions (owned - assigned):     []
```

로 보이게 됨. 즉, 모든 consumer 가 중지되지 않음!
