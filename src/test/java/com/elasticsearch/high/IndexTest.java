package com.elasticsearch.high;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description  java api 实现索引管理
 * @Auther pengl
 * @Version: 1.0
 * @Date 2021/5/12 17:23
 **/
@Slf4j
@SpringBootTest
public class IndexTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * @Description 创建索引
     * @Auther pengl
     * @Date 2021/5/12 19:52
     **/
    @Test
    public void testCreateIndex() throws IOException {
        //创建索引对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("itheima_book");
        //设置参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "0"));
        //指定映射1
        createIndexRequest.mapping(" {\n" +
                " \t\"properties\": {\n" +
                "            \"name\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"description\": {\n" +
                "              \"type\": \"text\"\n" +
                "           },\n" +
                "            \"price\":{\n" +
                "             \"type\":\"long\"\n" +
                "           },\n" +
                "           \"pic\":{\n" +
                "             \"type\":\"text\",\n" +
                "             \"index\":false\n" +
                "           }\n" +
                " \t}\n" +
                "}", XContentType.JSON);

        //指定映射2
        /*Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        createIndexRequest.mapping(mapping);*/

        //指定映射3
        /*XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        createIndexRequest.mapping(builder);*/

        //设置别名
        createIndexRequest.alias(new Alias("itheima_index_new"));

        // 额外参数
        //设置超时时间
        createIndexRequest.setTimeout(TimeValue.timeValueMinutes(2));
        //设置主节点超时时间
        createIndexRequest.setMasterTimeout(TimeValue.timeValueMinutes(1));
        //在创建索引API返回响应之前等待的活动分片副本的数量，以int形式表示
        createIndexRequest.waitForActiveShards(ActiveShardCount.from(2));
        createIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);

        //操作索引的客户端
        IndicesClient indices = restHighLevelClient.indices();
        //执行创建索引库
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);

        //得到响应（全部）
        boolean acknowledged = createIndexResponse.isAcknowledged();
        //得到响应 指示是否在超时前为索引中的每个分片启动了所需数量的碎片副本
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

        log.info("acknowledged================>{}", acknowledged);
        log.info("shardsAcknowledged================>{}", shardsAcknowledged);
    }

    /**
     * @Description 异步新增索引
     * @Auther pengl
     * @Date 2021/5/12 19:53
     **/
    @Test
    public void testCreateIndexAsync() throws IOException {
        //创建索引对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("itheima_book2");
        //设置参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "0"));
        //指定映射1
        createIndexRequest.mapping(" {\n" +
                " \t\"properties\": {\n" +
                "            \"name\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"description\": {\n" +
                "              \"type\": \"text\"\n" +
                "           },\n" +
                "            \"price\":{\n" +
                "             \"type\":\"long\"\n" +
                "           },\n" +
                "           \"pic\":{\n" +
                "             \"type\":\"text\",\n" +
                "             \"index\":false\n" +
                "           }\n" +
                " \t}\n" +
                "}", XContentType.JSON);

        //监听方法
        ActionListener<CreateIndexResponse> listener =
                new ActionListener<CreateIndexResponse>() {

                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        log.info("创建索引成功");
                        log.info(createIndexResponse.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.info("创建索引失败");
                        e.printStackTrace();
                    }
                };

        //操作索引的客户端
        IndicesClient indices = restHighLevelClient.indices();
        //执行创建索引库
        indices.createAsync(createIndexRequest, RequestOptions.DEFAULT, listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description 删除索引库
     * @Auther pengl
     * @Date 2021/5/12 19:55
     **/
    @Test
    public void testDeleteIndex() throws IOException {
        //删除索引对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("itheima_book2");
        //操作索引的客户端
        IndicesClient indices = restHighLevelClient.indices();
        //执行删除索引
        AcknowledgedResponse delete = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //得到响应
        boolean acknowledged = delete.isAcknowledged();
        log.info("acknowledged=========>{}", acknowledged);
    }

    /**
     * @Description 异步删除索引库
     * @Auther pengl
     * @Date 2021/5/12 19:56
     **/
    @Test
    public void testDeleteIndexAsync() throws IOException {
        //删除索引对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("itheima_book2");
        //操作索引的客户端
        IndicesClient indices = restHighLevelClient.indices();

        //监听方法
        ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                        log.info("删除索引成功");
                        log.info(deleteIndexResponse.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.info("删除索引失败");
                        e.printStackTrace();
                    }
                };
        //执行删除索引
        indices.deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description Indices Exists API
     * @Auther pengl
     * @Date 2021/5/12 19:58
     **/
    @Test
    public void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("itheima_book");
        request.local(false);//从主节点返回本地信息或检索状态
        request.humanReadable(true);//以适合人类的格式返回结果
        request.includeDefaults(false);//是否返回每个索引的所有默认设置

        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        log.info("exists========>{}", exists);
    }

    /**
     * @Description Indices Open API
     * @Auther pengl
     * @Date 2021/5/12 19:59
     **/
    @Test
    public void testOpenIndex() throws IOException {
        OpenIndexRequest request = new OpenIndexRequest("itheima_book");

        OpenIndexResponse openIndexResponse = restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
        boolean acknowledged = openIndexResponse.isAcknowledged();
        log.info("acknowledged=========>{}", acknowledged);
    }

    /**
     * @Description Indices Close API
     * @Auther pengl
     * @Date 2021/5/12 20:00
     **/
    @Test
    public void testCloseIndex() throws IOException {
        CloseIndexRequest request = new CloseIndexRequest("itheima_book");
        AcknowledgedResponse closeIndexResponse = restHighLevelClient.indices().close(request, RequestOptions.DEFAULT);
        boolean acknowledged = closeIndexResponse.isAcknowledged();
        log.info("acknowledged=========>{}", acknowledged);
    }

}
