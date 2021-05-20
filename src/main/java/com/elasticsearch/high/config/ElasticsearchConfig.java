package com.elasticsearch.high.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Description Elasticsearch配置类
 * @Auther pengl
 * @Version: 1.0
 * @Date 2021/5/10 15:00
 **/
@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.url}")
    private String url;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() {
        String[] urlSplit = url.split(",");
        HttpHost[] httpHostArray = new HttpHost[urlSplit.length];
        for (int i = 0; i < urlSplit.length; i++) {
            String item = urlSplit[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0],
                    Integer.parseInt(item.split(":")[1]), "http");
        }
        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }

}
