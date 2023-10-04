package com.nebois.streamfs.client;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.nebois.campus.client.domain.*;
import com.nebois.streamfs.client.domain.dto.ResponseEntity;
import com.nebois.streamfs.client.util.ZkManage;
import com.nebois.streamfs.client.util.OneGson;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.nebois.streamfs.client.util.HttpClientUtil.httpGet;

@Slf4j
public class EFileSystem extends java.io.FileSystem {

    private static final String FS_PATH = "/minfs"; // 文件系统主目录

    private ZooKeeper zooKeeper;

    private MetaServerInfo masterMetaServer;

    private MetaServerInfo slaveMetaServer;

    public EFileSystem() throws InterruptedException, KeeperException, IOException {
        new ZkManage();
        zooKeeper = ZkManage.getZk();
        // 如果不存在则创建主目录
        if (zooKeeper.exists(FS_PATH, null) == null) {
            zooKeeper.create(FS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zooKeeper.addWatch(FS_PATH, watchedEvent -> {
            log.info(watchedEvent.toString());
        }, AddWatchMode.PERSISTENT);
        // 如果不存在则创建子目录
        List<String> childPathList = new ArrayList<>(Arrays.asList("/metaServer", "/dataServer"));
        for (String childPath: childPathList) {
            String nodePath = FS_PATH + childPath;
            if (zooKeeper.exists(nodePath, null) == null) {
                zooKeeper.create(nodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        // 监听metaServer, 发生变化则重新分配主从节点
        zooKeeper.addWatch(FS_PATH + "/metaServer", watchedEvent -> {
            log.info(watchedEvent.toString());
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    assginMasterSlave();
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }, AddWatchMode.PERSISTENT);

        assginMasterSlave();
        log.info("文件系统初始化完成");
    }

    /**
     * 分配主从节点
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void assginMasterSlave() throws InterruptedException, KeeperException {
        String metaServerPath = "/minfs/metaServer";
        List<String> metaServerJsonList = getDataStringList(metaServerPath);
        Gson gson = new Gson();
        if (metaServerJsonList.size() == 1) {
            masterMetaServer = gson.fromJson(metaServerJsonList.get(0), MetaServerInfo.class);
            slaveMetaServer = null;
        } else if (metaServerJsonList.size() == 2) {
            masterMetaServer = gson.fromJson(metaServerJsonList.get(0), MetaServerInfo.class);
            slaveMetaServer = gson.fromJson(metaServerJsonList.get(1), MetaServerInfo.class);
        } else {
            // 其他情况暂时置空
            masterMetaServer = null;
            slaveMetaServer = null;
        }
    }

    /**
     * 打开文件, 可以理解为下载
     * @param path 文件在分布式存储系统中的地址
     * @return 分布式文件输入流
     * @throws IOException
     */
    public FSInputStream open(String path) throws IOException {
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/open";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        Type type = new TypeToken<ResponseEntity<StatInfo>>() {}.getType();
        ResponseEntity<StatInfo> responseEntity = OneGson.fromJson(responseJson, type);
        if (responseEntity.getCode() == 200) {
            List<ReplicaData> replicaList = responseEntity.getData().getReplicaData();
            return new FSInputStream(path, replicaList);
        } else {
            return null;
        }
    }

    /**
     * 创建文件, 可以理解为上传
     * @param path path 文件在分布式存储系统中的地址
     * @return 分布式文件输出流
     * @throws IOException
     */
    public FSOutputStream create(String path) throws IOException {
        // 访问metaServer, 获取到副本数据后传入FSOutputStream并返回
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/create";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        Type type = new TypeToken<ResponseEntity<StatInfo>>() {}.getType();
        ResponseEntity<StatInfo> responseEntity = OneGson.fromJson(responseJson, type);
        if (responseEntity.getCode() == 200) {
            List<ReplicaData> replicaList = responseEntity.getData().getReplicaData();
            return new FSOutputStream(path, replicaList, masterMetaServer);
        } else {
            return null;
        }
    }

    /**
     * 创建目录, 可多级创建
     * @param path 目录地址
     * @return 是否创建成功
     * @throws IOException
     */
    public boolean mkdir(String path) throws IOException {
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/mkdir";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        ResponseEntity responseEntity = OneGson.fromJson(responseJson, ResponseEntity.class);
        return responseEntity.getCode() == 200;
    }

    /**
     * 删除单个文件
     * @param path 文件地址
     * @return 是否删除成功
     * @throws IOException
     */
    public boolean delete(String path) throws IOException {
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/delete";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        ResponseEntity responseEntity = OneGson.fromJson(responseJson, ResponseEntity.class);
        return responseEntity.getCode() == 200;
    }

    /**
     * 获取单个文件的元数据
     * @param path 文件地址
     * @return 文件元数据
     * @throws IOException
     */
    public StatInfo getFileStats(String path) throws IOException {
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/stats";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        ResponseEntity<StatInfo> responseEntity = OneGson.fromJson(responseJson, new TypeToken<ResponseEntity<StatInfo>>(){} .getType());
        return responseEntity.getData();
    }

    /**
     * 获取目录下所有文件元数据列表
     * @param path 分布式路径
     * @return 文件元数据列表
     */
    public List<StatInfo> listFileStats(String path) throws IOException {
        String host = masterMetaServer.getHost();
        Integer port = masterMetaServer.getPort();
        String api = "/listdir";
        String params = "path=" + path;
        String responseJson = httpGet(host, port, api, params);
        ResponseEntity<List<StatInfo>> responseEntity = OneGson.fromJson(responseJson, new TypeToken<ResponseEntity<List<StatInfo>>>(){} .getType());
        return responseEntity.getData();
    }

    /**
     * 获取集群信息
     * @return 集群信息类
     * @throws InterruptedException
     * @throws KeeperException
     */
    public ClusterInfo getClusterInfo() throws InterruptedException, KeeperException {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterMetaServer(masterMetaServer);
        clusterInfo.setSlaveMetaServer(slaveMetaServer);
        clusterInfo.setDataServer(getDataServerList());
        return clusterInfo;
    }

    /**
     * 在zookeeper中获取对应节点地址下 所有子节点的data
     * @param dataPath 节点的路径
     * @return 子节点的data列表
     * @throws InterruptedException
     * @throws KeeperException
     */
    private List<String> getDataStringList(String dataPath) throws InterruptedException, KeeperException {
        List<String> dataList = new ArrayList<>();
        List<String> children = zooKeeper.getChildren(dataPath, false);
        for (String child: children) {
            byte[] dataByteArray = zooKeeper.getData(dataPath + "/" + child, false, null);
            dataList.add(new String(dataByteArray));
        }
        return dataList;
    }

    /**
     * 获取DataServer的信息列表
     * @return DataServer的信息列表
     * @throws InterruptedException
     * @throws KeeperException
     */
    private List<DataServerInfo> getDataServerList() throws InterruptedException, KeeperException {
        List<DataServerInfo> dataServerInfoList = new ArrayList<>();
        String dataServerPath = "/minfs/dataServer";
        List<String> dataServerStringList = getDataStringList(dataServerPath);
        for (String dataServerJson: dataServerStringList) {
            DataServerInfo dataServerInfo = OneGson.fromJson(dataServerJson, DataServerInfo.class);
            dataServerInfoList.add(dataServerInfo);
        }
        return dataServerInfoList;
    }
}
