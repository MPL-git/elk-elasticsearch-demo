package com.elasticsearch.high;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Random;

/**
 * @Description 输出日志
 * @Auther pengl
 * @Version: 1.0
 * @Date 2021/5/18 14:44
 **/
@Slf4j
@SpringBootTest
public class LogTest {


    @Test
    public void testLog() {
        Random random = new Random();
        while(true) {
            int userid=random.nextInt(10);
            log.info("userId:{},send:hello world.I am {}", userid, userid);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
