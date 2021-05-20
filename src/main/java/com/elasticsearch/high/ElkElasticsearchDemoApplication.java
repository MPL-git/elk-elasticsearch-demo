package com.elasticsearch.high;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@SpringBootApplication
public class ElkElasticsearchDemoApplication {

    public static void main(String[] args) throws UnknownHostException {
        ConfigurableApplicationContext application = SpringApplication.run(ElkElasticsearchDemoApplication.class, args);

        Environment env = application.getEnvironment();
        String ip = InetAddress.getLocalHost().getHostAddress();
        String port = env.getProperty("server.port");
        String path = env.getProperty("server.servlet.context-path");
        System.out.println("\n-----------------------------------------------------------------------------------\n\t" +
                "Application is running! Access URLs:\n\t" +
                "本地访问地址: \t\thttp://localhost:" + port + path + "\n\t" +
                "外网访问地址: \t\thttp://" + ip + ":" + port + path + "\n\t" +
                "swagger访问地址: \thttp://" + ip + ":" + port + path + "doc.html\n" +
                "-----------------------------------------------------------------------------------");
    }

}
