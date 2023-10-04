package com.nebois.streamfs.dataserver.services;

import com.google.gson.Gson;
import com.nebois.streamfs.dataserver.domain.DataServerMeta;
import com.nebois.streamfs.dataserver.domain.DirectoryStats;
import com.nebois.streamfs.dataserver.util.ZkManage;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.io.File;
import java.net.UnknownHostException;

@Slf4j
@Component
public class RegistService implements ApplicationRunner {

    @Resource
    private DataService dataService;

    @Resource
    private DataServerMeta meta;

    @Value("${data.path}")
    private String rootPath;

    private ZooKeeper zooKeeper;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        zooKeeper = ZkManage.getZk();
        registToCenter();
    }

    public void registToCenter() throws UnknownHostException, InterruptedException, KeeperException {

        //如果父目录找不到， 则创建之
        if (zooKeeper.exists("/minfs", null) == null) {
            zooKeeper.create("/minfs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zooKeeper.exists("/minfs/dataServer", null) == null) {
            zooKeeper.create("/minfs/dataServer", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //数据存放目录相关操作
        File dataDir = new File(rootPath);
        if (dataDir.exists() && dataDir.isDirectory()) {
            dataService.setDirStats(countDirInfo(dataDir)); //统计磁盘数据并设置DataService的useCapacity和fileTotal
            log.info("根目录: " + dataDir.getAbsolutePath());
        } else if (dataDir.mkdirs()){
            log.info("根目录: " + dataDir.getAbsolutePath());
        } else {
            log.info("根目录创建失败!");
        }

        //注册zookeeper
        String childPath = getChildPath(meta);
        zooKeeper.create(childPath, toByteArray(dataService.getDataServerInfo())
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat exists = zooKeeper.exists(childPath, false);
        if (exists != null) {
            log.info("已注册到Zookeeper");
        } else {
            log.error("注册失败");
        }

    }

    private DirectoryStats countDirInfo(File directory) {
        long fileTotal = 0;
        long useCapacity = 0;


        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    fileTotal++;
                    useCapacity += file.length();
                } else if (file.isDirectory()) {
                    DirectoryStats subdirStats = countDirInfo(file);
                    fileTotal += subdirStats.getFileTotal();
                    useCapacity += subdirStats.getUseCapacity();
                }
            }
        }

        return new DirectoryStats(fileTotal, useCapacity);
    }

    public void updataInfo() throws UnknownHostException, InterruptedException, KeeperException {
        String path = getChildPath(meta);
        zooKeeper.setData(path, toByteArray(dataService.getDataServerInfo()), zooKeeper.exists(path,true).getVersion());
    }

    private String getChildPath(DataServerMeta meta) {
        return "/minfs/dataServer/DS@" + meta.getPort();
    }

    public byte[] toByteArray(Object obj) {
        Gson gson = new Gson();
        String objStr = gson.toJson(obj);
        return objStr.getBytes();
    }

    @PreDestroy
    public void unregiste() throws InterruptedException, KeeperException {
        String path = "/minfs/dataServer/DS@" + meta.getPort();
        zooKeeper.delete(path, zooKeeper.exists(path,true).getVersion());
    }

}
