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
        LOGGER.info("==========================MyConsumer1 start=====================");

        if(Objects.isNull(message)){
            //接收到空消息，也表明此次消费成功
            return true;
        }
        LOGGER.info("MyConsumer1 received message: {}, topic: {}, tags: {}, keys: {}",
                message, message.getTopic(), message.getTags(), message.getKeys());

        //取出消息体
        byte[] messageBody = message.getBody();
        if(Objects.isNull(messageBody)){
            //接收到空消息，也表明此次消费成功
            return true;
        }

        /* 将byte数组类型转字符串 */
        String messageStr = StringUtils.byteArr2Str(messageBody);
        MessageData<User> messageData = JsonUtils.jsonStr2Obj(messageStr, new TypeReference<MessageData<User>>(){});
        LOGGER.info("幂等控制UUID: {}, 消息产生时间戳: {}",
                messageData.getUuid(), messageData.getTimestamp());

        //json字符串转Obj
        User user = JsonUtils.jsonObj2Obj(messageData.getData(), new TypeReference<User>(){});
        if(Objects.isNull(user)){
            //接收到空消息，也表明此次消费成功
            return true;
        }
        LOGGER.info("消息内容: {}", user);

        /**
         * TODO 具体的业务逻辑
         */

        LOGGER.info("==========================MyConsumer1 end=====================");
        return true;
    }
}
