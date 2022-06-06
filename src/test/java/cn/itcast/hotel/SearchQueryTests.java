package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/4 23:33
 */
@SpringBootTest
public class SearchQueryTests {


    private RestHighLevelClient client;

    @BeforeEach
    void before() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://47.111.103.239:9200")));
    }

    @AfterEach
    void clos() throws IOException {
        client.close();
    }


    @Test
    void testMatchAllQuery() throws IOException {

        //创建请求对象
        SearchRequest request = new SearchRequest("hotel");

        //创建查询资源数据对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //创建查询方式
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();

        sourceBuilder.query(query);

        request.source(sourceBuilder);

        //4.发送请求获得响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //获取目标JSON
        SearchHits hits = response.getHits();

        long value = hits.getTotalHits().value;
        System.out.println(value);
        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("文档ID=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }

    }


    @Test
    void testMatchQuery() throws IOException {
        //创建请求对象
        SearchRequest request = new SearchRequest("hotel");

        //创建查询资源数据对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //创建查询方式
//        MatchQueryBuilder query = QueryBuilders.matchQuery("all","如家");
        //多个匹配查询
        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery("如家", "brand", "name");

        sourceBuilder.query(query);

        request.source(sourceBuilder);

        //4.发送请求获得响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //获取目标JSON
        SearchHits hits = response.getHits();

        long value = hits.getTotalHits().value;
        System.out.println(value);
        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("文档ID=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }
    }

    @Test
    void testTermQuery() throws IOException {

        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //词条查询
//        TermQueryBuilder query = QueryBuilders.termQuery("city", "上海");
        //多词条查询
        TermsQueryBuilder query = QueryBuilders.termsQuery("city", "上海", "杭州");

        sourceBuilder.query(query);

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //总数
        System.out.println(hits.getTotalHits().value);

        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }

    }

    @Test
    void testRangeQuery() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //范围查询
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").gte(100).lte(500);

        sourceBuilder.query(query);

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //总数
        System.out.println(hits.getTotalHits().value);

        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }
    }

    @Test
    void testBoolQuery() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //构建查询方式
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "如家");

        query.must(matchQueryBuilder);
        query.filter(QueryBuilders.rangeQuery("price").lte(400));

        //放入请求资源
        sourceBuilder.query(query);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //总数
        System.out.println(hits.getTotalHits().value);

        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }
    }

    @Test
    void testQueryAndSort() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();

        //查询第一页
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.sort("price", SortOrder.DESC);

        sourceBuilder.query(query);

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //总数
        System.out.println(hits.getTotalHits().value);

        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(doc);
        }
    }

    @Test
    void testHighlight() throws IOException {

        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder query = QueryBuilders.matchQuery("all", "如家");

        //构建高亮数据
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        List<HighlightBuilder.Field> fields = highlightBuilder.fields();

        fields.add(new HighlightBuilder.Field("name").requireFieldMatch(false));

        sourceBuilder.query(query);
        sourceBuilder.highlighter(highlightBuilder);

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //总数
        System.out.println(hits.getTotalHits().value);

        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id=" + id);
            String jsonStr = hit.getSourceAsString();
            HotelDoc doc = JSON.parseObject(jsonStr, HotelDoc.class);

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("name");
            if (!ObjectUtils.isEmpty(highlightField)){

                Text[] fragments = highlightField.getFragments();

                StringBuffer stringBuffer = new StringBuffer();
                for (Text fragment : fragments) {
                    stringBuffer.append(fragment);
                }
                //覆盖之前对象的name值
                doc.setName(stringBuffer.toString());
            }
            System.out.println(doc);
        }
    }


}