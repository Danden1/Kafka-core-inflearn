package com.exmaple.kafka

import com.github.javafaker.Faker
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*


class PizzaMessage {
    //인자로 피자명 또는 피자가게 List와 Random 객체를 입력 받아서 random한 피자명 또는 피자 가게 명을 반환.
    private fun getRandomValueFromList(list: List<String>, random: Random): String {
        val size = list.size
        val index = random.nextInt(size)

        return list[index]
    }

    //random한 피자 메시지를 생성하고, 피자가게 명을 key로 나머지 정보를 value로 하여 Hashmap을 생성하여 반환.
    fun produceMsg(faker: Faker, random: Random, id: Int): HashMap<String, String> {
        val shopId = getRandomValueFromList(pizzaShop, random)
        val pizzaName = getRandomValueFromList(pizzaNames, random)

        val ordId = "ord$id"
        val customerName = faker.name().fullName()
        val phoneNumber = faker.phoneNumber().phoneNumber()
        val address = faker.address().streetAddress()
        val now = LocalDateTime.now()
        val message = String.format(
            "order_id:%s, shop:%s, pizza_name:%s, customer_name:%s, phone_number:%s, address:%s, time:%s",
            ordId, shopId, pizzaName, customerName, phoneNumber, address,
            now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN))
        )
        //System.out.println(message);
        val messageMap = HashMap<String, String>()
        messageMap["key"] = shopId
        messageMap["message"] = message

        return messageMap
    }

    companion object {
        // 피자 메뉴를 설정. getRandomValueFromList()에서 임의의 피자명을 출력하는 데 사용.
        private val pizzaNames = listOf(
            "Potato Pizza", "Cheese Pizza",
            "Cheese Garlic Pizza", "Super Supreme", "Peperoni"
        )

        //    private static final List<String> pizzaNames = List.of("고구마 피자", "치즈 피자",
        //            "치즈 갈릭 피자", "슈퍼 슈프림", "페페로니 피자");
        // 피자 가게명을 설정. getRandomValueFromList()에서 임의의 피자 가게명을 출력하는데 사용.
        private val pizzaShop = listOf(
            "A001", "B001", "C001",
            "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
            "O001", "P001", "Q001"
        )

        @JvmStatic
        fun main(args: Array<String>) {
            val pizzaMessage = PizzaMessage()
            // seed값을 고정하여 Random 객체와 Faker 객체를 생성.
            val seed: Long = 2022
            val random = Random(seed)
            val faker = Faker.instance(random)

            for (i in 0..59) {
                val message = pizzaMessage.produceMsg(faker, random, i)
                println("key:" + message["key"] + " message:" + message["message"])
            }
        }
    }
}