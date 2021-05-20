package com.elasticsearch.high;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@SpringBootTest
class ElkElasticsearchDemoApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    @Test
    public void testGetA() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("book", "1");
        //2.执行
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //3.获取结果
        log.info("id=====>{}", getResponse.getId());
        log.info("version=====>{}", getResponse.getVersion());
        log.info("source=====>{}", getResponse.getSourceAsString());
    }

    @Test
    public void testGetB() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("book", "1");
        //可选参数
        /*String[] includes = new String[]{"name", "description"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);*/

        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"name", "description"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        //2.执行
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //3.获取结果
        log.info("id=====>{}", getResponse.getId());
        log.info("version=====>{}", getResponse.getVersion());
        log.info("source=====>{}", getResponse.getSourceAsString());
    }

    @Test
    public void testGetC() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("book", "1");
        //可选参数
        /*String[] includes = new String[]{"name", "description"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);*/

        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"name", "description"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        //2.执行
        //同步查询
        //GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

        //异步查询
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            //成功
            @Override
            public void onResponse(GetResponse getResponse) {
                //3.获取结果
                log.info("id=====>{}", getResponse.getId());
                log.info("version=====>{}", getResponse.getVersion());
                log.info("source=====>{}", getResponse.getSourceAsString());
            }
            //失败
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        restHighLevelClient.getAsync(getRequest, RequestOptions.DEFAULT, listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetD() throws IOException {
        //1.构建请求
        GetRequest getRequest = new GetRequest("book", "1");
        //可选参数
        /*String[] includes = new String[]{"name", "description"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);*/

        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"name", "description"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        //2.执行
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //3.获取结果
        if(getResponse.isExists()) {
            log.info("id=====>{}", getResponse.getId());
            log.info("version=====>{}", getResponse.getVersion());
            log.info("source=====>{}", getResponse.getSourceAsString());
        }else {
            log.info("空值");
        }
    }


    @Test
    public void testAddA() throws IOException {
        //1.构建请求
        IndexRequest request = new IndexRequest("book");
        request.id("7");

        //方法1
        /*String jsonString = "{\n" +
                "    \"name\" : \"MySql开发\",\n" +
                "    \"description\" : \"MySql数据库。\",\n" +
                "    \"studymodel\" : \"201004\",\n" +
                "    \"price\" : 38.6,\n" +
                "    \"timestamp\" : \"2019-08-25 19:11:35\",\n" +
                "    \"pic\" : \"group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg\",\n" +
                "    \"tags\" : [\n" +
                "      \"MySql\",\n" +
                "      \"dev\"\n" +
                "    ]\n" +
                "  }";
        request.source(jsonString, XContentType.JSON);*/

        //方法2
        /*Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "C++开发");
        jsonMap.put("description", "C++");
        jsonMap.put("studymodel", "201004");
        jsonMap.put("price", "38.6");
        jsonMap.put("timestamp", "2019-08-25 19:11:35");
        jsonMap.put("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        String[] tags = new String[]{"C++", "dev"};
        jsonMap.put("tags", tags);
        request.source(jsonMap);*/

        //方法3
        /*XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("name", "C语言");
        builder.field("description", "C语言");
        builder.field("studymodel", "201004");
        builder.field("price", "38.6");
        builder.field("timestamp", "2019-08-25 19:11:35");
        builder.field("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        String[] tagsA = new String[]{"C语言", "dev"};
        builder.field("tags", tagsA);
        builder.endObject();
        request.source(builder);*/

        //方法4
        String[] tagsB = new String[]{"Go语言", "dev"};
        request.source("name", "Go语言",
                "description", "Go语言",
                "studymodel", "201004",
                "price", "38.6",
                "timestamp", "2019-08-25 19:11:35",
                "pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg",
                "tags", tagsB);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        //request.timeout("1s");

        //维护版本号
        request.version(2);
        request.versionType(VersionType.EXTERNAL);

        //2.执行
        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        //3.获取结果
        log.info("id=====>{}", indexResponse.getId());
        log.info("version=====>{}", indexResponse.getVersion());
        log.info("Result=====>{}", indexResponse.getResult());
    }

    @Test
    public void testAddB() throws IOException {
        //1.构建请求
        IndexRequest request = new IndexRequest("book");
        request.id("8");

        //方法2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "ELK");
        jsonMap.put("description", "ELK");
        jsonMap.put("studymodel", "201004");
        jsonMap.put("price", "38.6");
        jsonMap.put("timestamp", "2019-08-25 19:11:35");
        jsonMap.put("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        String[] tags = new String[]{"ELK", "dev"};
        jsonMap.put("tags", tags);
        request.source(jsonMap);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));

        //维护版本号
        request.version(2);
        request.versionType(VersionType.EXTERNAL);

        //2.执行
        //同步
        //IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        //异步
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                log.info("id=====>{}", indexResponse.getId());
                log.info("version=====>{}", indexResponse.getVersion());
                log.info("Result=====>{}", indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };

        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testAddC() throws IOException {
        //1.构建请求
        IndexRequest request = new IndexRequest("book");
        request.id("9");

        //方法2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "C++开发");
        jsonMap.put("description", "C++");
        jsonMap.put("studymodel", "201004");
        jsonMap.put("price", "38.6");
        jsonMap.put("timestamp", "2019-08-25 19:11:35");
        jsonMap.put("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        String[] tags = new String[]{"C++", "dev"};
        jsonMap.put("tags", tags);
        request.source(jsonMap);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));

        //维护版本号
        request.version(3);
        request.versionType(VersionType.EXTERNAL);

        //2.执行
        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        //3.获取结果
        log.info("id=====>{}", indexResponse.getId());
        log.info("version=====>{}", indexResponse.getVersion());
        log.info("Result=====>{}", indexResponse.getResult());

        if(indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
            log.info("操作：{}", indexResponse.getResult());
        }else if(indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
            log.info("操作：{}", indexResponse.getResult());
        }else {
            log.info("报错");
        }

    }

    @Test
    public void testAddD() throws IOException {
        //1.构建请求
        IndexRequest request = new IndexRequest("book");
        request.id("10");

        //方法2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "PHP开发");
        jsonMap.put("description", "PHP开发");
        jsonMap.put("studymodel", "201004");
        jsonMap.put("price", "38.6");
        jsonMap.put("timestamp", "2019-08-25 19:11:35");
        jsonMap.put("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        String[] tags = new String[]{"PHP开发", "dev"};
        jsonMap.put("tags", tags);
        request.source(jsonMap);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));

        //维护版本号
        request.version(2);
        request.versionType(VersionType.EXTERNAL);

        //2.执行
        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        //3.获取结果
        log.info("id=====>{}", indexResponse.getId());
        log.info("version=====>{}", indexResponse.getVersion());
        log.info("Result=====>{}", indexResponse.getResult());

        if(indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
            log.info("操作：{}", indexResponse.getResult());
        }else if(indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
            log.info("操作：{}", indexResponse.getResult());
        }else {
            log.info("报错");
        }

        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if(shardInfo.getTotal() != shardInfo.getSuccessful()) {
            log.info("处理成功分片数少于总分片数");
        }
        if(shardInfo.getFailed() > 0 ) {
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                log.info("错误原因：{}", failure.reason());
            }
        }
    }


    @Test
    public void testUpdateA() throws IOException {
        //1.构建请求
        UpdateRequest request = new UpdateRequest("book", "10");

        //方法2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("price", "38.7");
        request.doc(jsonMap);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));

        //重试次数
        request.retryOnConflict(3);

        //2.执行
        //同步
        UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);

        //3.获取结果
        log.info("id=====>{}", updateResponse.getId());
        log.info("version=====>{}", updateResponse.getVersion());
        log.info("Result=====>{}", updateResponse.getResult());

        if(updateResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
            log.info("操作：{}", updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
            log.info("操作：{}", updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
            log.info("操作：{}", updateResponse.getResult());
        }else if(updateResponse.getResult().equals(DocWriteResponse.Result.NOOP)){
            log.info("操作：{}", updateResponse.getResult());
        }else{
            log.info("报错");
        }
    }

    @Test
    public void testUpdateB() throws IOException {
        //1.构建请求
        UpdateRequest request = new UpdateRequest("book", "10");

        //方法2
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("price", "38.8");
        request.doc(jsonMap);

        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));

        //重试次数
        request.retryOnConflict(3);

        //2.执行
        //同步
        //UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        //异步
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                //3.获取结果
                log.info("id=====>{}", updateResponse.getId());
                log.info("version=====>{}", updateResponse.getVersion());
                log.info("Result=====>{}", updateResponse.getResult());

                if(updateResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
                    log.info("操作：{}", updateResponse.getResult());
                }else if(updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                    log.info("操作：{}", updateResponse.getResult());
                }else if(updateResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
                    log.info("操作：{}", updateResponse.getResult());
                }else if(updateResponse.getResult().equals(DocWriteResponse.Result.NOOP)){
                    log.info("操作：{}", updateResponse.getResult());
                }else{
                    log.info("报错");
                }
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };

        restHighLevelClient.updateAsync(request, RequestOptions.DEFAULT, listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testDelete() throws IOException {
        //1.构建请求
        DeleteRequest request = new DeleteRequest("book", "10");

        //2.执行
        //同步
        DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);

        //3.获取结果
        log.info("id=====>{}", deleteResponse.getId());
        log.info("version=====>{}", deleteResponse.getVersion());
        log.info("Result=====>{}", deleteResponse.getResult());

        if(deleteResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
            log.info("操作：{}", deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
            log.info("操作：{}", deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
            log.info("操作：{}", deleteResponse.getResult());
        }else if(deleteResponse.getResult().equals(DocWriteResponse.Result.NOOP)){
            log.info("操作：{}", deleteResponse.getResult());
        }else{
            log.info("报错");
        }
    }


    @Test
    public void testBulk() throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("test_index").id("1").source(XContentType.JSON, "field", "1"));
        request.add(new IndexRequest("test_index").id("2").source(XContentType.JSON, "field", "2"));
        request.add(new UpdateRequest("test_index","2").doc(XContentType.JSON, "field", "3"));
        request.add(new DeleteRequest("test_index").id("1"));

        BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);

        for (BulkItemResponse itemResponse : bulkResponse) {
            DocWriteResponse itemResponseResponse = itemResponse.getResponse();

            switch (itemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponseResponse;
                    indexResponse.getId();
                    System.out.println(indexResponse.getResult());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponseResponse;
                    updateResponse.getIndex();
                    System.out.println(updateResponse.getResult());
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponseResponse;
                    System.out.println(deleteResponse.getResult());
                    break;
            }
        }
    }

}
