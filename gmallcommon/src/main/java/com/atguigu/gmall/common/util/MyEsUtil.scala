package com.atguigu.gmall.common.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
//import org.apache.commons.beanutils.BeanUtils

/**
  * 往ES中插入数据
  */
object MyEsUtil {
    private val ES_HOST = "http://hadoop-01"
    private val ES_HTTP_PORT = 9200
    private var factory:JestClientFactory = null
    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
                .maxTotalConnection(20) //连接总数
                .connTimeout(10000).readTimeout(10000).build)

    }

  /***
    * 批量插入es
    * @param indexName 索引
    * @param list 数据
    */
  def indexBulk(indexName:String ,list :List[Any]): Unit ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list ) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }
//    val jestclient: JestClient = getClient

//    val result: BulkResult = jestclient.execute(bulkBuilder.build())
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    println(s"保存 = ${items.size()}")
    /*if(result.isSucceeded){
      println("保存成功:"+result.getItems.size())
    }*/
    close(jest)
  }


   /* def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
        val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
        for ( doc <- list ) {

            val indexBuilder = new Index.Builder(doc)
            if(idColumn!=null){
                val id: String = BeanUtils.getProperty(doc,idColumn)
                indexBuilder.id(id)
            }
            val index: Index = indexBuilder.build()
            bulkBuilder.addAction(index)
        }
        val jestclient: JestClient = getClient

        val result: BulkResult = jestclient.execute(bulkBuilder.build())
        if(result.isSucceeded){
            println("保存成功:"+result.getItems.size())
        }

    }*/

  /**
	* 插入一条数据
	*/
  def indexInsert(): Unit ={
	val jest: JestClient = getClient
	val source="{\n  \"name\":\"li4\",\n  \"age\":123,\n  \"amount\":250.1,\n  \"phone_num\":\"138***2123\"\n}"
	//值 索引
	val index: Index = new Index.Builder(source).index("gmall_test_zld").`type`("_doc").build()

	jest.execute(index) //插入
	close(jest)
  }




    def main(args: Array[String]): Unit = {
            /*val jest: JestClient = getClient
            val source="{\n  \"name\":\"li4\",\n  \"age\":123,\n  \"amount\":250.1,\n  \"phone_num\":\"138***2123\"\n}"
	  		//值 索引
            val index: Index = new Index.Builder(source).index("gmall_test").`type`("_doc").build()

            jest.execute(index) //插入
            close(jest)*/

	  indexInsert()
    }

}
