package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/3 20:24
 */
@SpringBootTest
public class HotelDocAddTest {

    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient client;

    @BeforeEach
    void buildClient() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://127.0.0.1:9200")));
    }

    @AfterEach
    void closeClient() throws IOException {
        client.close();
    }

    /***
     *  新增文档/全量修改
     * @throws IOException
     */
    @Test
    void testIndexReq() throws IOException {
        Hotel hotel = hotelService.getById(60223);
        HotelDoc doc = new HotelDoc(hotel);
        String json = JSON.toJSONString(doc);
        //创建添加文档请求对象
        IndexRequest request = new IndexRequest("hotel").id(doc.getId().toString());
        //构建请求体
        request.source(json, XContentType.JSON);

        //客户端发送添加文档请求
        client.index(request, RequestOptions.DEFAULT);
    }

}
