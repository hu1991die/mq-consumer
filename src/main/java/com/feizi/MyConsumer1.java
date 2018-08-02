package com.feizi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.feizi.starter.annotation.RocketMqConsumer;
import com.feizi.starter.core.RocketMqConsumerListener;
import com.feizi.starter.entity.MessageData;
import com.feizi.starter.entity.User;
import com.feizi.starter.util.JsonUtils;
import com.feizi.starter.util.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Created by feizi on 2018/6/27.
 */
@Component
@RocketMqConsumer(topic = "feizi_topic", selectorExpress = "tagA", consumerGroup = "my_consumer_group1")
public class MyConsumer1 implements RocketMqConsumerListener<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer1.class);

    @Override
    public boolean consume(Message message) {
        LOGGER.info("==========================consumer start=====================");

        if(Objects.isNull(message)){
            //接收到空消息，也表明此次消费成功
            return true;
        }
        LOGGER.info("MyConsumer received message: {}", message);
        LOGGER.info("消息主题topic: {}", message.getTopic());
        LOGGER.info("消息子主题tags: {}", message.getTags());
        LOGGER.info("消息keys: {}", message.getKeys());

        //取出消息体
        byte[] messageBody = message.getBody();
        if(Objects.isNull(messageBody)){
            //接收到空消息，也表明此次消费成功
            return true;
        }

        /**
         * TODO 具体的业务逻辑
         */

        String messageStr = StringUtils.byteArr2Str(messageBody);
        MessageData<User> messageData = JsonUtils.jsonStr2Obj(messageStr, new TypeReference<MessageData<User>>(){});
        LOGGER.info("UUID唯一值，用于消费幂等控制：: {}", messageData.getUuid());
        LOGGER.info("消息产生时间戳: {}", messageData.getTimestamp());

        //json字符串转Obj
        User user = JsonUtils.jsonObj2Obj(messageData.getData(), new TypeReference<User>(){});
        LOGGER.info("消息内容: {}", user);
        LOGGER.info("id: {}", user.getId());
        LOGGER.info("name: {}", user.getName());
        LOGGER.info("age: {}", user.getAge());

        LOGGER.info("==========================consumer end=====================");
        return true;
    }
}
