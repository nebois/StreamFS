package com.ksyun.campus.client.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ZkManage {
    private static final int SESSION_TIMEOUT = 2000; // 会话超时时间
    private static final String CONNECT_ADDRESS = "10.0.0.201:2181"; // 链接集群地址
    private static final CountDownLatch countDownLatch = new CountDownLatch(1); // 连接成功之前锁住

    private static ZooKeeper zk;

    public ZkManage() throws InterruptedException, IOException {
        zk = new ZooKeeper(CONNECT_ADDRESS, SESSION_TIMEOUT, watchedEvent -> {
            Watcher.Event.EventType type = watchedEvent.getType();
            if (type.equals(Watcher.Event.EventType.None)) {
                countDownLatch.countDown();
                log.info("连接成功");
            }
        });
        countDownLatch.await();
    }

    public static ZooKeeper getZk() {
        return zk;
    }

//    @PostConstruct
//    public void postCons() throws Exception {
//
//    }
}
