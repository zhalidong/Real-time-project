package com.atguigu.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall.canal.handler.CanalHandler;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by zld on 2019/9/23 0023.
 */
public class CanalApp {


    public static void main(String[] args) {

        //创建canal连接器 从canal读数据
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.126.189", 11111), "example", "", "");


        //不停的抓数
        while(true){
            //连接、订阅 、抓取数据
            canalConnector.connect();
            //什么库什么表
            canalConnector.subscribe("gmall1205.order_info");
            Message message = canalConnector.get(1);
            int size = message.getEntries().size();
            //没数据抓取
            if(size==0){
                try {
                    System.out.println("没有数据变化，休息一会");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }else{
                //有数据  遍历
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //抓取的数据过滤(只要数据EntryType.ROWDATA)  判断事件类型  只处理 行变化业务
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        //得到行集   把数据集进行反序列化
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange=null;

                        try {
                            //把storeValue转换 RowChange
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获得行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType  eventType = rowChange.getEventType();   //操作类型
                        String tableName = entry.getHeader().getTableName();//表名

                        //自己定义的工具
                        CanalHandler.handle(tableName,eventType,rowDatasList);
                    }




                }




            }

        }

    }

}
