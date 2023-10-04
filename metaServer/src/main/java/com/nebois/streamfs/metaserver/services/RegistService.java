package com.nebois.streamfs.metaserver.services;

import com.google.gson.Gson;
import com.nebois.streamfs.metaserver.domain.DataServerInfo;
import com.nebois.streamfs.metaserver.domain.MetaServiceInfo;
import com.nebois.streamfs.metaserver.util.ZkManage;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class RegistService implements ApplicationRunner {
    @Value("${server.port}")
    private Integer port;

    @Value("${data.path}")
    private String dataPath;

    private ZooKeeper zooKeeper;

    private List<DataServerInfo> dataServerList = new ArrayList<>();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        zooKeeper = ZkManage.getZk();
        // 在metaServer中创建元数据的根目录
        File dataDir = new File(dataPath);
        if (dataDir.exists() || dataDir.mkdir()) {
            log.info("根目录: " + dataDir.getAbsolutePath());
        } else {
            log.info("根目录创建失败!");
        }
        registToCenter();
        // 对dataServer进行监听, 如果发生变动, 立即进行metaService.dataServerList的维护
        zooKeeper.addWatch("/minfs/dataServer", watchedEvent -> {
            log.info(watchedEvent.toString());
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged
                    || watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    maintainDataServerList();
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }, AddWatchMode.PERSISTENT);
        maintainDataServerList();
    }

    /**
     * 维护DataServer列表
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void maintainDataServerList() throws InterruptedException, KeeperException {
        List<DataServerInfo> dataServerListFresh = new ArrayList<>();
        String dataServerPath = "/minfs/dataServer";
        List<String> children = zooKeeper.getChildren(dataServerPath, false);
        for (String child: children) {
            byte[] dataByteArray = zooKeeper.getData(dataServerPath + "/" + child, false, null);
            Gson gson = new Gson();
            DataServerInfo dataServerInfo = gson.fromJson(new String(dataByteArray), DataServerInfo.class);
            dataServerListFresh.add(dataServerInfo);
        }
        dataServerList = dataServerListFresh;
    }

    public List<DataServerInfo> getDataServerList() throws InterruptedException, KeeperException {
        maintainDataServerList();
        return dataServerList;
    }

    // 将本实例信息注册至zk中心，包含信息 ip、port
    public void registToCenter() throws UnknownHostException, InterruptedException, KeeperException {
        // 如果父目录找不到， 则创建之
        if (zooKeeper.exists("/minfs", null) == null) {
            zooKeeper.create("/minfs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zooKeeper.exists("/minfs/metaServer", null) == null) {
            zooKeeper.create("/minfs/metaServer", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zooKeeper.exists("/minfs/dataServer", null) == null) {
            zooKeeper.create("/minfs/dataServer", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String childPath = "/minfs/metaServer/MS@" + port;
        MetaServiceInfo info = new MetaServiceInfo(InetAddress.getLocalHost().getHostAddress());
        info.setPort(port);
        zooKeeper.create(childPath, toByteArray(info)
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat exists = zooKeeper.exists(childPath, false);
        if (exists != null) {
            log.info("已注册到Zookeeper");
        } else {
            log.error("注册失败");
        }
    }

    @PreDestroy
    public void unregiste() throws InterruptedException, KeeperException {
        String path = "/minfs/metaServer/MS@" + port;
        zooKeeper.delete(path, zooKeeper.exists(path,true).getVersion());
    }

    public byte[] toByteArray(Object obj) {
        Gson gson = new Gson();
        String objStr = gson.toJson(obj);
        return objStr.getBytes();
    }
}
