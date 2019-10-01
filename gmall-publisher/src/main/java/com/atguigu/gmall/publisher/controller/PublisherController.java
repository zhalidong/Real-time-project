package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * controller通过接收前端传过来的参数进行业务操作
 * Created by zld on 2019/9/22 0022.
 * 新增日活
 */


@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    //前端访问我的路径和里面所带的参数http://localhost:8070/realtime-total?date=2019-09-23  get请求  127.0.0.1 publisher

    /**
     * 我给前端的格式
     * [
     *  {"id":"dau","name":"新增日活","value":1200},
     *  {"id":"new_mid","name":"新增设备","value":233}
     * ]
     *
     */
    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date")String date){   //  参数注入到我这个参数上
        //定义一个list里面保存的hashmap
        List<Map> totalList = new ArrayList<>() ;
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");

        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");

        newMidMap.put("value",233);

        totalList.add(newMidMap);


        return JSON.toJSONString(totalList);
    }


    /**
     *
     * 分时统计
     * http://localhost:8070/realtime-hour?id=dau&date=2019-09-23
     * {"yesterday":{"钟点":数量},"today":{"钟点":数量}}

     {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
     "today":{"12":38,"13":1233,"17":123,"19":688 }}
     *
     *
     *
     */
    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id") String id,@RequestParam("date") String today){
        if("dau".equals(id)){
            //今天
            Map dauHourTDMap = publisherService.getDauHourMap(today);
            //求昨天的分时明细
            String yesterday = getYesterday(today);
            Map dauHourYDMap = publisherService.getDauHourMap(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today",dauHourTDMap);
            hourMap.put("yesterday",dauHourYDMap);
            //用json转成字符串
            return JSON.toJSONString(hourMap);

        }
        return null;


    }

    //今天日期减一天得到昨天
    private String getYesterday (String today){
        //日期转字符串
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday="";
        try {
            //字符串转日期
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            //日期转成字符串
            yesterday = simpleDateFormat.format(yesterdayDate);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return  yesterday;
    }



}
