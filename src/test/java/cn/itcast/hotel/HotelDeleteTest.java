package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/3 20:24
 */
@SpringBootTest
public class HotelDeleteTest {

    private RestHighLevelClient client;

    @BeforeEach
    void buildClient() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://47.111.103.239:9200")));
    }

    @AfterEach
    void closeClient() throws IOException {
        client.close();
    }

    @Test
    void testDeleteReq() throws IOException {

        //创建删除文档请求对象
        DeleteRequest request = new DeleteRequest("hotel").id("36934");

        //客户端发送删除请求
        client.delete(request,RequestOptions.DEFAULT);

    }

}
