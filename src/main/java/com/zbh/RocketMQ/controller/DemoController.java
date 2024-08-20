package com.zbh.RocketMQ.controller;


import com.zbh.RocketMQ.config.RocketMqService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/demo")
public class DemoController {


    @Resource
    private RocketMqService rocketMqService;

    @RequestMapping("/send")
    public String hello() throws Exception {
        rocketMqService.sendSyncMessage("Topic1","hahah22a");
        return "index";
    }
}
