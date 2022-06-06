package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
