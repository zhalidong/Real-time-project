package com.atguigu.gmall.dw.mock.utils;

/*import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;*/

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 把日志变成远程请求的工具 日志发送工具
 */
public class LogUploader {




    public static void sendLogStream(String log){
        try{
            //不同的日志类型对应不同的URL

            //URL url =new URL("http://logserver:8080/log");
            //nginx服务器的地址  向nginx发送数据
            URL url =new URL("http://hadoop-01/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");
            //允许上传数据
            conn.setDoOutput(true);
            //设置请求的头信息,设置内容类型为JSON 表单数据
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            //http://xxxxx.com/xx?log={json...}
            out.write(("log="+log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}
