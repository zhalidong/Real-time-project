package com.atguigu.gmall.publisher.service;

import java.util.Map;

/**
 * Created by zld on 2019/9/22 0022.
 */


public interface PublisherService {

//查询当日总数
public Integer getDauTotal(String date);


//取分时间段明细 每个小时的数据量
    public Map getDauHourMap(String date);

}
