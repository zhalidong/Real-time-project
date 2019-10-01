package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.common.constant.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zld on 2019/9/22 0022.
 */

@Service
public class PublisherServiceimpl implements com.atguigu.gmall.publisher.service.PublisherService{

    @Autowired
    JestClient jestClient;


    @Override
    public Integer getDauTotal(String date) {
        //方法一
        String query="{\n" +
                "    \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2019-09-23\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "   }\n" +
                "  }";

        //方法二 等同于以上直接写sql
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());

        //查询 方法二
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        //方法一
//        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();


        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            //获得结果条数
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }


    @Override
    public Map getDauHourMap(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();

        Map dauHourMap = new HashMap();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket:buckets){
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHourMap.put(key,count);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return dauHourMap;
    }
}
