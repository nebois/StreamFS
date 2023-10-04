package com.nebois.streamfs.dataserver.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
public class ZkManage {
    private static int SESSION_TIMEOUT;
    @Value("${zookeeper.timeout}")
    public void setSessionTimeout(int timeout) {
        SESSION_TIMEOUT = timeout;
    }

    private static String CONNECT_ADDRESS;
    @Value("${zookeeper.address}")
    public void setConnectAddress(String address) {
        CONNECT_ADDRESS = address;
    }

    private static final CountDownLatch countDownLatch = new CountDownLatch(1); // 连接成功之前锁住

    private static ZooKeeper zk;

    public static ZooKeeper getZk() {
        return zk;
    }

    @PostConstruct
    public void postCons() throws Exception {
        zk = new ZooKeeper(CONNECT_ADDRESS, SESSION_TIMEOUT, watchedEvent -> {
            Watcher.Event.EventType type = watchedEvent.getType();
            if (type.equals(Watcher.Event.EventType.None)) {
                countDownLatch.countDown();
                log.info("连接成功");
            }
        });
        countDownLatch.await();
    }
}
