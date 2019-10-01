package com.atguigu.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {


    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")
        //读取配置文件中的配置参数 kafka.broker.list
        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName:String): Properties ={
        val prop=new Properties();  //读取配置文件 hashmap
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }
}
