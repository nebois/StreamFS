package com.ksyun.campus.dataserver.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.ksyun.campus.dataserver.domain.DataServerInfo;
import com.ksyun.campus.dataserver.domain.DataServerMeta;
import com.ksyun.campus.dataserver.domain.DirectoryStats;
import com.ksyun.campus.dataserver.domain.dto.ByteArrayDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.utils.Base64;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class DataService {
    @Resource
    private DataServerMeta dataServerMeta;

    @Value("${data.path}")
    private String rootPath;

    private DirectoryStats dirStats = new DirectoryStats(0L, 0L);

    private Cache<String, RandomAccessFile> fileMap = Caffeine.newBuilder()
            //初始数量
            .initialCapacity(50)
            //最大条数
            .maximumSize(100)
            //最后一次读或写操作后经过指定时间过期
            .expireAfterAccess(5, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, RandomAccessFile>() {
                @Override
                public void onRemoval(@Nullable String s, @Nullable RandomAccessFile randomAccessFile, @NonNull RemovalCause removalCause) {
                    try {
                        randomAccessFile.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .build();


    public boolean write(String path, byte[] data, Integer length) throws IOException{
        // 写本地
        File file = new File(rootPath + path);
        // 父目录不存在则创建
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) return false;
        if (!file.exists()) {
            if (!file.createNewFile()) return false;
            else {
                dirStats.addFileTotal(1);
            }
        }
        dirStats.addUseCapacity(length);
        RandomAccessFile fileIO = fileMap.getIfPresent(path);
        if (fileIO == null) {
            fileMap.put(path, new RandomAccessFile(file, "rw"));
            fileIO = fileMap.getIfPresent(path);
        }
        // 跳转到文件末尾实现追加写
        fileIO.seek(fileIO.length());
        fileIO.write(data, 0, length);
        return true;
    }

    public ByteArrayDTO read(String path, int offset, int length) throws IOException {
        File file = new File(rootPath + path);
        if (!file.exists()) {
            return null;
        }
        RandomAccessFile fileIO = fileMap.getIfPresent(path);
        if (fileIO == null) {
            fileMap.put(path, new RandomAccessFile(file, "rw"));
            fileIO = fileMap.getIfPresent(path);
        }
        ByteArrayDTO byteArrayDTO = new ByteArrayDTO();
        byte[] data = new byte[length];
        fileIO.seek(offset);
        int read = fileIO.read(data, 0, length);
        // 如果读下个字节返回-1，说明读到文件末尾，则设置对应属性返回
        if (fileIO.read() == -1) {
            byteArrayDTO.setIsEndOfFile(true);
        }
        byteArrayDTO.setFileSize(fileIO.length());
        byteArrayDTO.setByteData(Base64.encodeBase64String(data));
        return byteArrayDTO;
    }

    public DataServerInfo getDataServerInfo() throws UnknownHostException {
        DataServerInfo dataServerInfo = new DataServerInfo();
        dataServerInfo.setHost(InetAddress.getLocalHost().getHostAddress());
        dataServerInfo.setPort(dataServerMeta.getPort());
        dataServerInfo.setCapacity(dataServerMeta.getMaxCapacity());
        dataServerInfo.setUseCapacity(dirStats.getUseCapacity());
        dataServerInfo.setFileTotal(dirStats.getFileTotal());
        return dataServerInfo;
    }

    protected void setDirStats(DirectoryStats countDirInfo) {
        this.dirStats = countDirInfo;
    }

    public boolean delete(String path) throws IOException {
        File file = new File(rootPath + path);
        if (!file.exists()) return true;
        RandomAccessFile randomAccessFile = fileMap.getIfPresent(path);
        if (randomAccessFile != null) {
            fileMap.invalidate(path);
            randomAccessFile.close();
        }
        return file.delete();
    }
}
