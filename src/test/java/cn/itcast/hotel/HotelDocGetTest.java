package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
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
public class HotelDocGetTest {

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
    void testGetReq() throws IOException {

        //创建查询文档请求对象
        GetRequest request = new GetRequest("hotel").id("36934");

        //客户端获取文档响应对象
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        String jsonStr = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(jsonStr, HotelDoc.class);

        System.out.println("hotelDoc = " + hotelDoc);

    }

}
