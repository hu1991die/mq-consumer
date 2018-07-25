package com.feizi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.feizi.starter.annotation.RocketMqConsumer;
import com.feizi.starter.core.RocketMqConsumerListener;
import com.feizi.starter.entity.MessageData;
import com.feizi.starter.entity.User;
import com.feizi.starter.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Created by feizi on 2018/6/27.
 */
@Component
@RocketMqConsumer(topic = "feizi_topic2", consumerGroup = "my_consumer_group2")
public class MyConsumer2 implements RocketMqConsumerListener<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer2.class);

    @Override
    public void consume(String message) {
        LOGGER.info("==========================consumer start=====================");

        if(Objects.isNull(message)){
            //接收到空消息，也表明此次消费成功
            return;
        }

        /**
         * TODO 具体的业务逻辑
         */
        MessageData<User> messageData = JsonUtils.jsonStr2Obj(message, new TypeReference<MessageData<User>>(){});
        LOGGER.info("MyConsumer1 received message: {}", messageData);
        LOGGER.info("UUID唯一值，用于消费幂等控制：: {}", messageData.getUuid());
        LOGGER.info("消息产生时间戳: {}", messageData.getTimestamp());
        LOGGER.info("消息内容: {}", messageData.getData());

        //json字符串转Obj
        User user = JsonUtils.jsonObj2Obj(messageData.getData(), new TypeReference<User>(){});
        LOGGER.info("具体内容: {}", user);
        LOGGER.info("id: {}", user.getId());
        LOGGER.info("name: {}", user.getName());
        LOGGER.info("age: {}", user.getAge());

        LOGGER.info("==========================consumer end=====================");
    }
}
