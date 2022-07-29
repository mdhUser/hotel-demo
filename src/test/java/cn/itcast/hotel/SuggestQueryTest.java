package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @description:
 * @author: Maxwell
 * @email: maodihui@foxmail.com
 * @date: 2022/6/6 21:21
 */
public class SuggestQueryTest {


    private RestHighLevelClient client;

    @BeforeEach
    void before() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://192.168.112.128:9200")));
    }

    @AfterEach
    void clos() throws IOException {
        client.close();
    }

    @Test
    void testSuggestQuery() throws IOException {

        //构建请求对象
        SearchRequest request = new SearchRequest("test");

        //构建请求资源
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //构建自动填充对象
        sourceBuilder.suggest(new SuggestBuilder()
                .addSuggestion("titleSuggest"
                        , SuggestBuilders.completionSuggestion("title")
                                .prefix("s")
                                .skipDuplicates(true)
                                .size(10))

        );
        //设置数据
        request.source(sourceBuilder);
        //获取响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析自动填充内容
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestion = suggest.getSuggestion("titleSuggest");
        //输出结果
        suggestion.getOptions().forEach(option -> {
            Text text = option.getText();
            System.out.println(text);
        });
    }

}