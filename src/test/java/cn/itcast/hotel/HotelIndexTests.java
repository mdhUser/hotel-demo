package cn.itcast.hotel;

import cn.itcast.hotel.constant.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class HotelIndexTests {

    private RestHighLevelClient client;

    @BeforeEach
    void buildClient() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://47.111.103.239:9200")));
    }


    @AfterEach
    void closeClient() throws IOException {
        client.close();
    }


    /**
     * 创建索引库
     *
     * @throws IOException
     */
    @Test
    void testCreateIndex() throws IOException {
        //1.创建请求对象--创建索引库请求对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");

        //2.设置source数据 （json-索引库映射）
        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);

        //3.调用client发送请求创建索引库和映射
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 查询索引库
     *
     * @throws IOException
     */
    @Test
    void testGetIndex() throws IOException {
        //1.创建请求对象--get索引库请求对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        //判断库是否存在
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引库
     *
     * @throws IOException
     */
    @Test
    void testDeleteIndex() throws IOException {
        //1.创建求求对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //删除库
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

}
