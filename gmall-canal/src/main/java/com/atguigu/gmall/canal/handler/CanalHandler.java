package com.atguigu.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.common.constant.GmallConstant;
import com.google.common.base.CaseFormat;

import java.util.List;


/**
 * Created by zld on 2019/9/24 0024.  逻辑 抓取新增操作
 */
public class CanalHandler {
    public  static  void handle(String tableName , CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList){
        //抓取新增操作
        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT.equals(eventType)){
            //下单操作
            for (CanalEntry.RowData rowData : rowDatasList) {  //行集展开
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject=new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {  //列集展开
                    System.out.println("---------zld--------");
                    System.out.println(column.getName()+":::"+column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertyName,column.getValue());
                }
//                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }

        }


    }

}
