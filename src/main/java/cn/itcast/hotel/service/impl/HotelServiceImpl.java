package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;


    @Override
    public PageResult search(RequestParams params) {

        try {
            //创建请求对象
            SearchRequest request = getSearchRequest(params);
            //获取响应对象
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //解析响应
            return parseResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 实现查询酒店过滤
     *
     * @param params
     * @return
     */
    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {

        try {
            SearchRequest request = getSearchRequest(params);

            //设置桶查询
            SearchSourceBuilder sourceBuilder = request.source();
            sourceBuilder.size(0);
            //请求桶数据
            String brandCount = "brandCount";
            String cityCount = "cityCount";
            String starNameCount = "starNameCount";
            AggregationBuilders.terms(brandCount).field("brand").size(100);
            AggregationBuilders.terms(cityCount).field("city").size(100);
            AggregationBuilders.terms(starNameCount).field("starName").size(100);
            //获取查询结果
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //获取聚合结果
            Aggregations aggregations = response.getAggregations();
            List<String> brands = getKeys(aggregations, brandCount);
            List<String> cities = getKeys(aggregations, cityCount);
            List<String> starNames = getKeys(aggregations, starNameCount);
            return new HashMap<String, List<String>>() {
                {
                    put("brand", brands);
                    put("city", cities);
                    put("starName", starNames);
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String prefix) {

        try {
            SearchRequest request = new SearchRequest("hotel");

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            sourceBuilder.suggest(new SuggestBuilder().addSuggestion("suggestion_field",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(20)
            ));

            request.source(sourceBuilder);

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestion = suggest.getSuggestion("suggestion_field");

            List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
            List<String> suggestList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(options)) {
                options.forEach(option -> {
                    String text = option.getText().toString();
                    suggestList.add(text);
                });
            }

            return suggestList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            // 1.准备Request
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            // 2.发送请求
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            // 0.根据id查询酒店数据
            Hotel hotel = getById(id);
            // 转换为文档类型
            HotelDoc hotelDoc = new HotelDoc(hotel);

            // 1.准备Request对象
            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
            // 2.准备Json文档
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // 3.发送请求
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取桶keys
     *
     * @param aggregations
     * @return
     */
    private List<String> getKeys(Aggregations aggregations, String bucketName) {
        Terms terms = aggregations.get(bucketName);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        if (!CollectionUtils.isEmpty(buckets)) {
            return buckets.stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
        }
        return null;
    }

    /**
     * 请求构建函数
     *
     * @param params
     * @return
     */
    private SearchRequest getSearchRequest(RequestParams params) {
        //创建请求对象
        SearchRequest request = new SearchRequest("hotel");
        //创建请求资源
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //创建请求方法
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        if (StringUtils.isEmpty(params.getKey())) {
            query.must(QueryBuilders.matchAllQuery());
        } else {
            query.must(QueryBuilders.matchQuery("all", params.getKey()));
        }

        //设置过滤器
        if (!StringUtils.isEmpty(params.getCity())) {
            query.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        if (!StringUtils.isEmpty(params.getStarName())) {
            query.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        if (!StringUtils.isEmpty(params.getBrand())) {
            query.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            query.filter(QueryBuilders.rangeQuery("price").lte(params.getMaxPrice()).gte(params.getMinPrice()));
        }

        //方法得分查询
        FunctionScoreQueryBuilder scoreQuery = QueryBuilders.functionScoreQuery(
                query,
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(10)
                        )
                }
        );

        sourceBuilder.query(scoreQuery);
        //构建分页
        int from = (params.getPage() - 1) * params.getSize();
        sourceBuilder.from(from);
        sourceBuilder.size(params.getSize());

        //设置排序
        if (!StringUtils.isEmpty(params.getLocation())) {
            sourceBuilder.sort(SortBuilders.geoDistanceSort("location", new GeoPoint(params.getLocation())).unit(DistanceUnit.KILOMETERS).order(SortOrder.ASC));
            sourceBuilder.sort("price", SortOrder.ASC);
        }
        request.source(sourceBuilder);
        return request;
    }

    /**
     * 解析响应体
     *
     * @param response
     * @return
     */
    private PageResult parseResponse(SearchResponse response) {

        SearchHits hits = response.getHits();

        long total = hits.getTotalHits().value;

        SearchHit[] hitsHits = hits.getHits();

        List<HotelDoc> hotelDocs = Arrays.stream(hitsHits).map(h -> {
            HotelDoc doc = JSON.parseObject(h.getSourceAsString(), HotelDoc.class);
            Object[] sortValues = h.getSortValues();
            if (sortValues.length > 1) {
                Object sortValue = sortValues[0];
                doc.setDistance(sortValue);
            }
            return doc;
        }).collect(Collectors.toList());

        return new PageResult(total, hotelDocs);
    }


}
