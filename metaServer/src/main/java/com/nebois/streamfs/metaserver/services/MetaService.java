package com.nebois.streamfs.metaserver.services;
import com.nebois.streamfs.metaserver.domain.DataServerInfo;
import com.nebois.streamfs.metaserver.domain.FileType;
import com.nebois.streamfs.metaserver.domain.ReplicaData;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import com.nebois.streamfs.metaserver.domain.StatInfo;
import com.nebois.streamfs.metaserver.domain.dto.ResponseEntity;
import com.nebois.streamfs.metaserver.util.HttpClientUtil;
import com.nebois.streamfs.metaserver.util.OneGson;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class MetaService {

    @Resource
    private RegistService registService;

    @Value("${data.path}")
    private  String rootPath;

    /**
     * 通过zk内注册的ds列表，选择出来负载最低的三个ds，用来后续的wirte
     * @return
     */
    public List<ReplicaData> pickDataServer() throws InterruptedException, KeeperException {
        List<ReplicaData> replicaDataList = new ArrayList<>();
        // 负载方式, 选择容量最低的三个副本写入
        List<DataServerInfo> dataServerList = registService.getDataServerList();
        dataServerList.sort((x, y) -> (int) (x.getUseCapacity() - y.getUseCapacity()));
        for (DataServerInfo info : dataServerList) {
            ReplicaData replicaData = new ReplicaData();
            // 目前只设置path为副本DataServer的地址与端口, 其他字段作用尚待考虑
            if (info.getUseCapacity() >= info.getCapacity()) continue; // 容量不足的不选入
            replicaData.setDsNode(info.getHost() + ":" + info.getPort());
            replicaDataList.add(replicaData);
            if (replicaDataList.size() >= 3) break;
        }
        return replicaDataList;
    }

    public List<StatInfo> listdir(String path) throws IOException {
        // 在MetaServer上虚拟出一个根目录为/的文件系统, 使用树型结构存放各文件的元数据
        // 返回打开目录的所有平级结构
        String dirPath = rootPath + path;
        File file = new File(dirPath);
        if (! file.isDirectory()) throw new IOException("不是目录或不存在");

        List<StatInfo> fileStatList = new ArrayList<>();
        String[] fileList = file.list();
        for (String fileName: fileList) {
            if (!fileName.endsWith(".meta")) continue;
            File thisFile = new File(dirPath + "/" + fileName);
            if (thisFile.exists()) {
                fileStatList.add(metaFileToStatInfo(thisFile));
            }
        }
        return fileStatList;
    }

    private StatInfo metaFileToStatInfo(File thisFile) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (InputStreamReader streamReader = new InputStreamReader(Files.newInputStream(thisFile.toPath()), StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(streamReader);) {
            String content = bufferedReader.readLine();
            builder.append(content);
        }
        return OneGson.fromJson(builder.toString(), StatInfo.class);
    }


    public boolean mkdirs(String path) throws IOException {
        String dirPath = rootPath + path;
        File dirOrigin = new File(path); // 目录在分布式系统中的抽象
        File dir = new File(dirPath); // 目录本身
        File dirMeta = new File(dirPath + ".meta"); // 目录的meta文件
        if (dir.exists()) {
            return false;
        }

        StatInfo fileStat = new StatInfo();
        fileStat.setPath(path);
        fileStat.setSize(0L);
        fileStat.setMtime(System.currentTimeMillis());
        fileStat.setType(FileType.Directory);

        if (dir.mkdir()) {
            return createMetaFile(dirMeta, fileStat);
        }

        File parent = dirOrigin.getParentFile();
        if (parent == null) return false; // 父目录获取为空
        if (!mkdirs(parent.getPath()) && !parent.exists()) return false; // 父目录不存在且创建失败
        if (!dir.mkdir()) return false; // 此目录创建失败
        return createMetaFile(dirMeta, fileStat); // 元数据创建情况
    }

    private boolean createMetaFile(File metaFile, StatInfo fileStat) throws IOException {
        // 检查目标文件是否存在，不存在则创建, 创建不成功返回
        if (!metaFile.createNewFile()) {
            return false;
        }

        String dirStatJson = OneGson.toJson(fileStat);
        try (FileWriter writer = new FileWriter(metaFile);) {
            // 向目标文件中写入内容
            writer.append(dirStatJson);
            writer.flush();
        }
        return true;
    }

    // 加锁
    public synchronized StatInfo createFile(String path) throws IOException, InterruptedException, KeeperException {
        String realPath = rootPath + path + ".meta";
        File file = new File(realPath);
        if (file.exists()) {
            // 文件已存在, 返回这个文件的元数据
            return getStats(path);
        }

        File parentDir = file.getParentFile();
        File fileOrigin = new File(path);
        // 如果父目录不存在且父目录创建失败, 则返回空
        if (!parentDir.exists() && !mkdirs(fileOrigin.getParentFile().getPath())) {
            return null;
        }

        StatInfo fileStat = new StatInfo();
        fileStat.setPath(path);
        fileStat.setSize(0L);
        fileStat.setMtime(System.currentTimeMillis());
        fileStat.setType(FileType.File);
        fileStat.setReplicaData(pickDataServer());

        if (createMetaFile(file, fileStat)) return fileStat;
        else return null;
    }

    public StatInfo getStats(String path) throws IOException {
        String realPath = rootPath + path + ".meta";
        File file = new File(realPath);
        if (!file.exists()) return null;
        return metaFileToStatInfo(file);
    }

    public boolean delete(String path) throws IOException {
        File dir = new File(rootPath + path);
        if (dir.exists() && !dir.delete()) return false;

        String realPath = rootPath + path + ".meta";
        File file = new File(realPath);
        if (!file.exists()) return false;
        // 读取文件获取副本位置
        StatInfo statInfo = metaFileToStatInfo(file);
        List<ReplicaData> replicaData = statInfo.getReplicaData();
        // 通知对应的dataServer去删除
        if (replicaData != null) {
            for (ReplicaData replica: replicaData) {
                String dsNode = replica.getDsNode();
                String responseJson = HttpClientUtil.httpGet("http://" + dsNode + "/delete?path=" + path);
                ResponseEntity responseEntity = OneGson.fromJson(responseJson, ResponseEntity.class);
                if (responseEntity.getCode() != 200) {
                    throw new IOException("删除失败: " + dsNode);
                }
            }
        }
        return file.delete();
    }

    public StatInfo open(String path) throws IOException {
        String realPath = rootPath + path + ".meta";
        File file = new File(realPath);
        if (!file.exists()) {
            return null;
        } else {
            //返回这个文件的元数据
            return getStats(path);
        }
    }

    public boolean commitWrite(String path, long length) throws IOException {
        String realPath = rootPath + path + ".meta";
        File file = new File(realPath);

        StatInfo fileStat = getStats(path);
        if (fileStat == null) return false;

        fileStat.setMtime(System.currentTimeMillis());
        fileStat.setSize(length);

        String dirStatJson = OneGson.toJson(fileStat);
        try (FileWriter writer = new FileWriter(file);) {
            // 向目标文件中写入内容
            writer.append(dirStatJson);
            writer.flush();
        }
        return true;
    }
}
