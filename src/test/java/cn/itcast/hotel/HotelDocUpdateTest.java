package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.List;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/3 20:24
 */
@SpringBootTest
public class HotelDocUpdateTest {

    @Autowired
    private IHotelService hotelService;

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
    void testUpdateReq() throws IOException {

        //创建增量修改文档请求对象
        UpdateRequest request = new UpdateRequest("hotel", "60223");

        //构建请求体
        request.doc("name", "Maxwell's Hotel", "price", "7378");

        //发送修改请求
        client.update(request, RequestOptions.DEFAULT);

    }

    /**
     * 批量导入
     */
    @Test
    void testBulkReq() throws IOException {

        List<Hotel> hotels = hotelService.list();

        BulkRequest request = new BulkRequest();

        //构建
        for (Hotel hotel : hotels) {
            HotelDoc doc = new HotelDoc(hotel);
            IndexRequest req = new IndexRequest("hotel").id(hotel.getId().toString());
            req.source(JSON.toJSONString(doc), XContentType.JSON);
            request.add(req);
        }

        //发送
        client.bulk(request,RequestOptions.DEFAULT);

    }


}
