package com.zbh.RocketMQ.config;


import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class RocketMqService {

    private static final Logger log = LogManager.getLogger(RocketMqService.class);


    private final Long TIMEOUT_MILES = 1000L;

    @Resource
    private RocketMQTemplate rocketMQTemplate;


    public void sendMessage(final String topic, final String msg) {
        rocketMQTemplate.asyncSend(topic,msg,new SendCallback() {


            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("rockerMq发送异步消息成功，消息体{},sendResult{}",msg,sendResult);
            }

            @Override
            public void onException(Throwable t) {
                log.error("rocketMQ发送异步消息异常，转同步，消息体：{}",msg,t);
                SendResult sendResult = rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(msg).build());
                log.info("rockerMq发送同步消息，消息体{},sendResult{}",msg, JSONObject.toJSONString(sendResult));
            }
        });
    }


    public void sendSyncMessage( String topic,  String msg) throws Exception {
        log.info("rockerMq发送同步消息，消息体{},topic{}",msg, topic);
        SendResult sendResult = rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(msg).build());
        log.info("rockerMq发送同步消息，消息体{},返回结果{}",msg, JSONObject.toJSONString(sendResult));
        if(!SendStatus.SEND_OK.equals(sendResult.getSendStatus())){
            throw new Exception("rockerMq发送同步消息失败,msg：",new Throwable(msg));
        }
    }

    public void sendDelayMessage( String topic, String msg, int delayLevel) throws Exception {
        log.info("rockerMq发送延迟消息，消息体{},topic{}",msg, topic);
        SendResult sendResult = rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(msg).build(),TIMEOUT_MILES,delayLevel);
        log.info("rockerMq发送延迟消息，消息体{},返回结果{}",msg, JSONObject.toJSONString(sendResult));
        if(!SendStatus.SEND_OK.equals(sendResult.getSendStatus())){
            throw new Exception("rockerMq发送延迟消息失败,msg：",new Throwable(msg));
        }
    }

    public void sendOrderlyMessage(String topic ,String msg,String hash){
        try {
            rocketMQTemplate.syncSendOrderly(topic,MessageBuilder.withPayload(msg).build(),hash);
        }catch (Exception e){

        }
    }
}
