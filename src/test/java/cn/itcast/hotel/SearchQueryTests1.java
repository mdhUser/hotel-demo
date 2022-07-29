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
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.WeightBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/7/28 15:51
 */
@SpringBootTest
public class SearchQueryTests1 {

    private RestHighLevelClient client;

    @BeforeEach
    void buildClient() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://192.168.112.128:9200")));
    }

    @AfterEach
    void closeClient() throws IOException {
        client.close();
    }


    /**
     * match 查询
     *
     * @throws IOException
     */
    @Test
    void testMatchAll() throws IOException {

        SearchRequest request = new SearchRequest("hotel");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery("如家", "brand", "name");
        sourceBuilder.query(query);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        long value = response.getHits().getTotalHits().value;
        System.out.println(value);

        SearchHit[] hits = response.getHits().getHits();
        Arrays.stream(hits).forEach(h -> {
            String id = h.getId();
            System.out.printf("文档id=%s%n", id);
            String sourceAsString = h.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println(hotelDoc);
        });
    }

    /**
     * 精确查询
     *
     * @throws IOException
     */
    @Test
    void testTermAndRangeQuery() throws IOException {

        //1.创建请求对象
        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //term 查询
//        TermQueryBuilder query = QueryBuilders.termQuery("city", "杭州");
        //range 查询
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").gte(100).lte(250);
        sourceBuilder.query(query);

        //分页，排序
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.sort("price", SortOrder.ASC);

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        long value = response.getHits().getTotalHits().value;
        System.out.println(value);
        SearchHit[] hits = response.getHits().getHits();
        Arrays.stream(hits).forEach(h -> {
            String id = h.getId();
            System.out.printf("文档id=%s%n", id);
            String sourceAsString = h.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println(hotelDoc);
        });
    }

    /**
     * 布尔查询
     */
    @Test
    void testBoolQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        //创建查询条件
        query.must(QueryBuilders.matchQuery("name", "如家"));
        query.filter().add(QueryBuilders.rangeQuery("price").lte(400));
        //地理查询
        query.filter().add(QueryBuilders.geoDistanceQuery("location").point(31.21, 121.5).distance("10", DistanceUnit.KILOMETERS));
        sourceBuilder.query(query);
        sourceBuilder.sort("price", SortOrder.ASC);
        request.source(sourceBuilder);

        //解析响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        long value = response.getHits().getTotalHits().value;
        System.out.println(value);
        SearchHit[] hits = response.getHits().getHits();
        Arrays.stream(hits).forEach(h -> {
            String id = h.getId();
            System.out.printf("文档id=%s%n", id);
            String sourceAsString = h.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println(hotelDoc);
        });
    }

    /**
     * 算分查询
     */
    @Test
    void testScoreQuery() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("all", "外滩"),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("brand", "如家"),
                                new WeightBuilder().setWeight(10))
                }).boostMode(CombineFunction.MULTIPLY);

        sourceBuilder.query(functionScoreQueryBuilder);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit h : response.getHits().getHits()) {
            String id = h.getId();
            System.out.printf("文档id=%s%n", id);
            System.out.printf("分数score=%s%n", h.getScore());
            String sourceAsString = h.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println(hotelDoc);
        }
    }

    /**
     * 查询高亮设置
     *
     * @throws IOException
     */
    @Test
    void testHighLight() throws IOException {

        //创建请求
        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //侯建查询方式
        MatchQueryBuilder query = QueryBuilders.matchQuery("all", "如家");
        sourceBuilder.query(query);
        sourceBuilder.highlighter(new HighlightBuilder().field("name").requireFieldMatch(false).preTags("<font color='red'>").postTags("</font>"));
        request.source(sourceBuilder);

        //解析请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit h : response.getHits().getHits()) {
            String id = h.getId();
            System.out.printf("文档id=%s%n", id);
            String sourceAsString = h.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            //获取高亮
            Map<String, HighlightField> highlightFields = h.getHighlightFields();
            HighlightField field = highlightFields.get("name");
            if (!ObjectUtils.isEmpty(field)) {
                Text[] fragments = field.getFragments();
                StringBuilder stringBuffer = new StringBuilder();
                for (Text fragment : fragments) {
                    stringBuffer.append(fragment);
                }
                hotelDoc.setName(stringBuffer.toString());
            }
            System.out.println(hotelDoc);
        }

    }

}
