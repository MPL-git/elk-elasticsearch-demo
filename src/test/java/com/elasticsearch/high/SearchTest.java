package com.elasticsearch.high;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

/**
 * @Description 查询
 * @Auther pengl
 * @Version: 1.0
 * @Date 2021/5/13 14:12
 **/
@Slf4j
@SpringBootTest
public class SearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * @Description 搜索全部记录
     * @Auther pengl
     * @Date 2021/5/13 14:14
     **/
    @Test
    public void testSearchAll() throws IOException {
        /*GET book/_search
        {
            "query": {
                "match_all": {}
             }
        }*/
        //1构建搜索请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //获取某些字段
        //searchSourceBuilder.fetchSource(new String[]{"name"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        //2执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //3获取结果
        SearchHits hits = searchResponse.getHits();

        //数据数据
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description 搜索分页
     * @Auther pengl
     * @Date 2021/5/13 14:23
     **/
    @Test
    public void testSearchPage() throws IOException {
        /*GET book/_search
        {
            "query": {
              "match_all": {}
           },
            "from": 0,
            "size": 2
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //第几页
        int page = 1;
        //每页几个
        int size = 2;
        //下标计算
        int from = (page - 1) * size;

        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();

        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description ids搜索
     * @Auther pengl
     * @Date 2021/5/13 14:31
     **/
    @Test
    public void testSearchIds() throws IOException {
        /*GET / book / _search
        {
            "query":{
                "ids" :{
                    "values" : ["1", "4", "100"]
                }
            }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds("1", "2", "100"));

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();

        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description match搜索
     * @Auther pengl
     * @Date 2021/5/13 14:34
     **/
    @Test
    public void testSearchMatch() throws IOException {
        /*GET /book/_search
        {
            "query": {
               "match": {
                "description": "java程序员"
            }
          }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("description", "java程序员"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description term 搜索
     * @Auther pengl
     * @Date 2021/5/13 14:39
     **/
    @Test
    public void testSearchTerm() throws IOException {
        /*GET / book / _search
        {
            "query":{
                "term":{
                    "description":"java程序员"
                }
            }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("description", "java程序员"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description multi_match搜索
     * @Auther pengl
     * @Date 2021/5/13 14:40
     **/
    @Test
    public void testSearchMultiMatch() throws IOException {
        /*GET / book / _search
        {
            "query":{
                "multi_match":{
                    "query":"java程序员",
                    "fields": ["name", "description"]
                }
            }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("java程序员", "name", "description"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description bool搜索
     * @Auther pengl
     * @Date 2021/5/13 14:43
     **/
    @Test
    public void testSearchBool() throws IOException {
        /*GET /book/_search
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": "java程序员",
                                "fields": [
                                    "name",
                                    "description"
                                ]
                            }
                        }
                    ],
                    "should": [
                        {
                            "match": {
                                "studymodel": "201001"
                            }
                        }
                    ]
                }
            }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构建multiMatch请求
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("java程序员", "name", "description");
        //构建match请求
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("studymodel", "201001");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description filter搜索
     * @Auther pengl
     * @Date 2021/5/13 14:54
     **/
    @Test
    public void testSearchFilter() throws IOException {
        /*GET /book/_search
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": "java程序员",
                                "fields": ["name","description"]
                            }
                        }
                    ],
                    "should": [
                        {
                            "match": {
                                "studymodel": "201001"
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "price": {
                                "gte": 50,
                                "lte": 90
                            }
                        }

                    }
                }
            }
        }*/
        SearchRequest searchRequest = new SearchRequest("book");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构建multiMatch请求
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("java程序员", "name", "description");
        //构建match请求
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("studymodel", "201001");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);

        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(50).lte(90));

        searchSourceBuilder.query(boolQueryBuilder);


        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

    /**
     * @Description sort搜索
     * @Auther pengl
     * @Date 2021/5/13 14:58
     **/
    @Test
    public void testSearchSort() throws IOException {
        /*GET /book/_search
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": "java程序员",
                                "fields": ["name","description"]
                            }
                        }
                    ],
                    "should": [
                        {
                            "match": {
                                "studymodel": "201001"
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "price": {
                                "gte": 50,
                                "lte": 90
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    "price": {
                        "order": "asc"
                    }
                }
            ]
        }*/
        //1构建搜索请求
        SearchRequest searchRequest = new SearchRequest("book");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构建multiMatch请求
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("java程序员", "name", "description");
        //构建match请求
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("studymodel", "201001");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);

        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(50).lte(90));

        searchSourceBuilder.query(boolQueryBuilder);

        //按照价格升序
        searchSourceBuilder.sort("price", SortOrder.ASC);

        searchRequest.source(searchSourceBuilder);

        //2执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //3获取结果
        SearchHits hits = searchResponse.getHits();

        //数据数据
        SearchHit[] searchHits = hits.getHits();
        log.info("--------------------------");
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            String description = sourceAsMap.get("description").toString();
            Double price = sourceAsMap.get("price") == null ? null : Double.parseDouble(sourceAsMap.get("price").toString());
            log.info("id:{}", id);
            log.info("score:{}", score);
            log.info("name:{}", name);
            log.info("description:{}", description);
            log.info("price:{}", price);
            log.info("==========================");
        }
    }

}
