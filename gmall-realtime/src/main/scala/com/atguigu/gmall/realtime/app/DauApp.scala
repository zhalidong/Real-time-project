package com.atguigu.gmall.realtime.app


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.common.util.MyEsUtil
import com.atguigu.gmall.realtime.bean.Startuplog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * sparkstreaming 消费kafka的数据
  * 指标日活
  */
object DauApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
        val ssc = new  StreamingContext(sparkConf,Seconds(5))

        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
        /**
		  * foreachRDD通常用来把SparkStream运行得到的结果保存到外部系统比如HDFS、Mysql、Redis等等
          *
		  *
          */
        //    inputDstream.foreachRDD{rdd=>
        //      println(rdd.map(_.value()).collect().mkString("\n"))
        //    }

        // 转换格式处理 把每一行变成一个case  class
        val startuplogStream: DStream[Startuplog] = inputDstream.map { record =>
            val jsonStr: String = record.value()
            val startuplog: Startuplog = JSON.parseObject(jsonStr, classOf[Startuplog])
            val date = new Date(startuplog.ts)
            val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
            val dateArr: Array[String] = dateStr.split(" ")
            startuplog.logDate = dateArr(0)
            startuplog.logHour = dateArr(1).split(":")(0)
            startuplog.logHourMinute = dateArr(1)

            startuplog
        }
        // 利用redis进行去重过滤 transform这个算子的分两部分执行
        val filteredDstream: DStream[Startuplog] = startuplogStream.transform { rdd =>
            println("过滤前："+rdd.count())
            //driver端执行  //按照周期性循环执行
            val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
            val jedis: Jedis = RedisUtil.getJedisClient
            val key = "dau:" + curdate
            val dauSet: util.Set[String] = jedis.smembers(key)
            val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)

		  	//executor执行
            val filteredRDD: RDD[Startuplog] = rdd.filter { startuplog =>
                //executor
                val dauSet: util.Set[String] = dauBC.value
                !dauSet.contains(startuplog.mid)
            }
            println("过滤后："+filteredRDD.count())
            filteredRDD

        }

        //去重思路;把相同的mid的数据分成一组 ，每组取第一个  前5秒和后5秒的重复数据我们可以进行处理 在本批次中有重复数据
        val groupbyMidDstream: DStream[(String, Iterable[Startuplog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
        val distinctDstream: DStream[ Startuplog ] = groupbyMidDstream.flatMap { case (mid, startulogItr) =>
            startulogItr.take(1)
        }



        // 保存到redis中
        distinctDstream.foreachRDD{rdd=>
            //driver
            // redis  type set
            // key  dau:2019-06-03    value : mids
            rdd.foreachPartition{startuplogItr=>		//executer
                //executor
                val jedis: Jedis = RedisUtil.getJedisClient
                val list: List[Startuplog] = startuplogItr.toList
                for ( startuplog<- list ) {
                    val key="dau:"+startuplog.logDate
                    val value=startuplog.mid
                    jedis.sadd(key,value)				//redis本来就有去重的功能
                    println(startuplog) //往es中保存
                }
                //批量插入 es 查询语句GET gmall1205_dau/_search
                MyEsUtil.indexBulk(GmallConstant.ES_INDEX_DAU,list)
                jedis.close()
            }

            //      rdd.foreach { startuplog =>   //executor
            //        val jedis: Jedis = RedisUtil.getJedisClient
            //        val key="dau:"+startuplog.logDate
            //        val value=startuplog.mid
            //        jedis.sadd(key,value)
            //        jedis.close()
            //      }



        }




        ssc.start()

        ssc.awaitTermination()


    }


}
