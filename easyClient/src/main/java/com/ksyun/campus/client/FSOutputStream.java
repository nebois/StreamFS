package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.MetaServerInfo;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.domain.dto.ResponseEntity;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.OneGson;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.ksyun.campus.client.util.HttpClientUtil.httpGet;

public class FSOutputStream extends OutputStream {
    private String path;

    private MetaServerInfo metaServer;

    private List<ReplicaData> replicaList;

    private long writeTotal = 0;

    public FSOutputStream(String path, List<ReplicaData> replicaList,  MetaServerInfo metaServer) {
        this.path = path;
        this.replicaList = replicaList;
        this.metaServer = metaServer;
    }


    @Override
    public void write(int b) throws IOException {
        byte[] singleByte = new byte[1];
        singleByte[0] = (byte) b;
        write(singleByte, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @SneakyThrows
    @Override
    public void write(byte[] b, int off, int len) {
        List<Future> requestResultList = new ArrayList<>();
        List<String> hostList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        try {
            for (ReplicaData replica: replicaList) {
                Callable<ResponseEntity> commitCreate = () -> writeOnOneServer(replica.dsNode, b, off, len);
                Future<ResponseEntity> entityFuture = executorService.submit(commitCreate);
                requestResultList.add(entityFuture);
                hostList.add(replica.dsNode);
            }
            for (Future future: requestResultList) {
                ResponseEntity response = (ResponseEntity) future.get();
                if (response.getCode() != 200) throw new IOException("写入失败");
            }
        } finally {
            executorService.shutdown();
        }
        writeTotal += len; // 写入成功, 增加写入总字数
    }

    private ResponseEntity writeOnOneServer(String dsNode, byte[] b, int off, int len) throws IOException {
        String params = "path=" + path + "&length=" + len;
        byte[] data = new byte[len];
        for (int i = 0; off + i < b.length && i < len; i++) {
            data[i] = b[off + i];
        }
        // 暂时缺省offset字段
        String responseJson = HttpClientUtil.httpPostData("http://" + dsNode + "/write?" + params, data);
        return OneGson.fromJson(responseJson, ResponseEntity.class);
    }

    @Override
    public void close() throws IOException {
        String params = "path=" + path + "&length=" + writeTotal;
        String responseJson = httpGet("http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/write?" + params);
        ResponseEntity response = OneGson.fromJson(responseJson, ResponseEntity.class);
        if (response.getCode() != 200) throw  new IOException("提交写请求出错");
        super.close();
    }
}
