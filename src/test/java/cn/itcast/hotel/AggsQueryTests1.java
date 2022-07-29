package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/7/28 22:46
 */
@SpringBootTest
public class AggsQueryTests1 {

    private RestHighLevelClient client;

    @BeforeEach
    void before() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://192.168.112.128:9200")));
    }

    @AfterEach
    void clos() throws IOException {
        client.close();
    }

    /**
     * 桶聚合
     *
     * @throws IOException
     */
    @Test
    void testAggsQuery() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().size(0).
                //设置bucket
                        aggregation(AggregationBuilders.terms("brandCount").field("brand").size(20)
                        .order(BucketOrder.count(true)));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析响应
        Aggregations aggregations = response.getAggregations();
        Terms terms = aggregations.get("brandCount");
        terms.getBuckets().forEach(bucket -> {
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(key + "::" + docCount);
        });
    }

    /**
     * 筒中筒 王中王
     *
     * @throws IOException
     */
    @Test
    void testSubAggsQuery() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        //设置桶中桶
        request.source().size(0)
                .aggregation(AggregationBuilders.terms("brandCount").field("brand").size(20)
                        .subAggregation(AggregationBuilders.terms("cityCount").field("city").size(20))
                );

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Terms terms = aggregations.get("brandCount");
        //解析桶
        terms.getBuckets().forEach(bucket -> {
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            Terms subTerms = bucket.getAggregations().get("cityCount");
            subTerms.getBuckets().forEach(b -> {
                String key1 = b.getKeyAsString();
                long docCount1 = b.getDocCount();
                System.out.println(key1 + "::" + docCount1);
                System.out.println("===============");
            });
            System.out.println(key + "::" + docCount);
            System.out.println("===========================================");
        });
    }


}
