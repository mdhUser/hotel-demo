package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/6 19:00
 */
public class AggsQueryTests {

    private RestHighLevelClient client;

    @BeforeEach
    void before() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://47.111.103.239:9200")));
    }

    @AfterEach
    void clos() throws IOException {
        client.close();
    }

    /**
     * 桶聚合基本使用
     *
     * @throws IOException
     */
    @Test
    void testAggsQuery() throws IOException {

        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        //构建数据聚合对象
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("brandCount").field("brand").size(20);

        //设置数据
        sourceBuilder.aggregation(aggregationBuilder);

        request.source(sourceBuilder);

        //获取响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析数据统计结果
        Aggregations aggregations = response.getAggregations();

        Terms terms = aggregations.get("brandCount");

        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket -> {
            String keyAsString = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(keyAsString + "::" + docCount);
        });
    }


    /**
     * 桶套桶
     */
    @Test
    void testSubAggsQuery() throws IOException {

        SearchRequest request = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("brandCount").field("brand").size(20);

        aggregationBuilder.subAggregation(AggregationBuilders.terms("cityCount").field("city")
                .size(20));


        sourceBuilder.aggregation(aggregationBuilder);

        request.source(sourceBuilder);

        //获取响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析数据统计结果
        Aggregations aggregations = response.getAggregations();

        Terms terms = aggregations.get("brandCount");

        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket -> {
            String keyAsString = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(keyAsString + "::" + docCount);

            Aggregations subAgg = bucket.getAggregations();
            Terms subTerms = subAgg.get("cityCount");

            List<? extends Terms.Bucket> buckets1 = subTerms.getBuckets();
            buckets1.forEach(b -> {
                String keyAsString1 = b.getKeyAsString();
                long docCount1 = b.getDocCount();
                System.out.println(keyAsString1 + "::" + docCount1);
                System.out.println("====================");
            });
        });

    }

}