package com.atguigu.gmall.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * 控制器Controller负责处理由DispatcherServlet分发的请求，
 * 它把用户请求的数据经过业务处理层处理之后封装成一个Model，
 * 然后再把该Model返回给对应的View进行展示
 */



//找到请求路径http://logserver/log ? log={json...}

@RestController //@Controller + @ResponseBody
public class LoggerController {
    /**
     * Spring 2.5 引入了 @Autowired 注释，
     * 它可以对类成员变量、方法及构造函数进行标注，
     * 完成自动装配的工作。
     * 通过 @Autowired的使用来消除 set ，get方法
     */
    @Autowired      //自动装配
    KafkaTemplate<String,String> kafkaTemplate;

    //使用log4j
    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;
    //@RequestMapping(value = "/log",method = RequestMethod.POST) RequestMapping是一个用来处理请求地址映射的注解，可用于类或方法上
    @PostMapping("/log")            //处理post请求
    //@ResponseBody       把logJson当做字符串  而不是logJson.jsp
    //把参数注入到log
    /**
     * 在SpringMvc后台进行获取数据，一般是两种。
     1.request.getParameter(“参数名”)
     2.用@RequestParam注解获取
     */
    public String dolog(@RequestParam("log") String logJson){
        //补时间戳 数据到我这在封装时间戳
        //1.把json变成对象 jsonObject就是一个hashmap
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());
        //落盘到logfile log4j落盘 输出到文件 把info级别的日志输出 将JSON对象转化为JSON字符
        logger.info(jsonObject.toJSONString());

        //日志拆分 发送kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }


        //System.out.println(logJson);
        return "success";
    }

}
