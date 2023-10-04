package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.domain.dto.ByteArrayDTO;
import com.ksyun.campus.dataserver.domain.dto.ResponseEntity;
import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.services.RegistService;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;

@RestController("/")
public class DataController {

    @Resource
    private DataService dataService;

    @Resource
    private RegistService registService;

    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity writeFile(
            @RequestHeader(required = false) String fileSystem,
            @RequestParam String path,
            @RequestParam(required = false) Long offset,
            @RequestParam Integer length,
            @RequestBody byte[] data) throws IOException, InterruptedException, KeeperException {
        if (dataService.write(path, data, length)) {
            registService.updataInfo();
            return ResponseEntity.ok();
        } else {
            return ResponseEntity.fail("写入失败");
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("read")
    public ResponseEntity readFile(
            @RequestHeader(required = false) String fileSystem,
            @RequestParam String path,
            @RequestParam int offset,
            @RequestParam int length) throws IOException {
        ByteArrayDTO read = dataService.read(path, offset, length);
        if (read != null) {
            return ResponseEntity.ok().data(read);
        } else {
            return ResponseEntity.fail("读取失败");
        }

    }

    @RequestMapping("delete")
    public ResponseEntity delete(@RequestParam String path) throws IOException {
        if (dataService.delete(path)) return ResponseEntity.ok();
        else return  ResponseEntity.fail("删除失败: " + path);
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
