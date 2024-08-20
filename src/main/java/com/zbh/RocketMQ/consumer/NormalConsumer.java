package com.zbh.RocketMQ.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
@RocketMQMessageListener(consumerGroup = "Consumer1",topic = "Topic1",consumeThreadNumber = 5)
public class NormalConsumer implements RocketMQListener<MessageExt> {


    private static final Logger log = LoggerFactory.getLogger(NormalConsumer.class);

    @Override
    public void onMessage(MessageExt e) {

        try{
        byte[] body = e.getBody();
        String data = new String(body);
        log.info("接收到消息，data:{}",data);
        log.info("处理消息，data:{}",data);
        int i =1/0;
        log.info("处理完成，data:{}",data);
        }catch (Exception exception){
            exception.printStackTrace();
        }
    }
}
