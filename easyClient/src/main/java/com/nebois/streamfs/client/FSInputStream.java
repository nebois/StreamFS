package com.nebois.streamfs.client;

import com.google.gson.reflect.TypeToken;
import com.nebois.streamfs.client.common.Commons;
import com.nebois.streamfs.client.domain.ReplicaData;
import com.nebois.streamfs.client.domain.dto.ByteArrayDTO;
import com.nebois.streamfs.client.domain.dto.ResponseEntity;
import com.nebois.streamfs.client.util.HttpClientUtil;
import com.nebois.streamfs.client.util.OneGson;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.utils.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Random;

@Slf4j
public class FSInputStream extends InputStream {
    private byte[] buffer = new byte[Commons.READ_BUFFER_SIZE];

    // 当前data读取到的位置
    private int bufferOffset = 0;

    // 文件指针: 表示目前缓冲区中首个字节在实际文件中的offset
    private Long dataPointer = 0L;

    // 是否是文件结尾
    private boolean isEndOfFile = false;

    private String path;

    private Long fileSize;

    private List<ReplicaData> replicaList;

    public FSInputStream(String path, List<ReplicaData> replicaList) throws IOException {
        this.path = path;
        this.replicaList = replicaList;
        getData(0);
    }

    @Override
    public int read() throws IOException {
        byte[] singleByte = new byte[1];
        if (read(singleByte, 0, 1) == -1) return -1;
        return singleByte[0];
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // 维护读取的数
        int cnt = 0;
        if (len > Commons.READ_BUFFER_SIZE) throw new IOException("读取长度超出缓存数组范围");
        if (off + len > b.length) throw new IOException("读取长度超出目标数组范围");

        if (len == 0) return 0;
        if (dataPointer + bufferOffset >= fileSize) return -1;

        // 如果需要读取的数据超过当前数据
        if (bufferOffset + len > buffer.length) {
            if (isEndOfFile) {
                // 是文件末尾的话, 有多少读多少
                len = buffer.length - bufferOffset - 1;
            } else {
                // 从远端拉取数据更新缓存, 拉取off是文件中真实的off
                getData(dataPointer + bufferOffset);
            }
        }
        for (; cnt < len; cnt++, bufferOffset++) {
            if (dataPointer + bufferOffset >= fileSize) break;
            b[off + cnt] = buffer[bufferOffset];
        }
        // len = 0时返回0;
        if (len == 0 || cnt == 0) cnt = -1;
        return cnt;
    }

    private void getData(long off) throws IOException {
        ReplicaData availableReplica = getReplica();
        String dsNode = availableReplica.getDsNode();
        log.info("数据来源:" + dsNode);
        String responseJson = HttpClientUtil.httpGet("http://" + dsNode +
                "/read?path=" + path +
                "&offset=" + off +
                "&length=" + Commons.READ_BUFFER_SIZE
        );
        Type type = new TypeToken<ResponseEntity<ByteArrayDTO>>() {}.getType();
        ResponseEntity<ByteArrayDTO> response = OneGson.fromJson(responseJson, type);
        ByteArrayDTO byteArrayDTO = response.getData();
        buffer = Base64.decodeBase64(byteArrayDTO.getByteData());
        isEndOfFile = byteArrayDTO.getIsEndOfFile();
        fileSize = byteArrayDTO.getFileSize();
        // 获取到新的数据之后将off置为0
        bufferOffset = 0;
        dataPointer = off;
    }

    // 暂时使用随机
    private ReplicaData getReplica() {
        Random random = new Random(System.currentTimeMillis());
        int randomInt = random.nextInt();
        randomInt = randomInt<0?-randomInt:randomInt;
        return replicaList.get(randomInt % replicaList.size());
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
