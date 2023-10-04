package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.dto.ResponseEntity;
import com.ksyun.campus.metaserver.services.MetaService;

import org.apache.zookeeper.KeeperException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@RestController("/")
public class MetaController {

    @Resource
    private MetaService metaService;

    @RequestMapping("stats")
    public ResponseEntity stats(@RequestParam String path) throws IOException {
        StatInfo stats = metaService.getStats(path);
        if (stats != null) {
            return ResponseEntity.ok().data(stats);
        } else {
            return ResponseEntity.fail().desc("文件元数据获取失败或不存在");
        }
    }

    @RequestMapping("create")
    public ResponseEntity createFile(@RequestParam String path) throws IOException, InterruptedException, KeeperException {
        StatInfo fileStat = metaService.createFile(path);
        // 返回创建的文件元数据
        if (fileStat != null) return ResponseEntity.ok().data(fileStat);
        else return ResponseEntity.fail("文件创建失败");
    }

    @RequestMapping("mkdir")
    public ResponseEntity mkdir(@RequestParam String path) throws IOException {
         if (metaService.mkdirs(path)) return ResponseEntity.ok();
         else return ResponseEntity.fail("创建失败");
    }

    @RequestMapping("listdir")
    public ResponseEntity listdir(@RequestParam String path) throws IOException {
        List<StatInfo> statInfoList = metaService.listdir(path);
        //为空说明没有东西
        return ResponseEntity.ok().data(statInfoList);
    }

    @RequestMapping("delete")
    public ResponseEntity delete(@RequestParam String path) throws IOException {
        if (metaService.delete(path)) {
            return ResponseEntity.ok();
        } else {
            return ResponseEntity.fail("删除失败");
        }

    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     * @param path
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity commitWrite(@RequestParam String path, @RequestParam long length) throws IOException {
        if (metaService.commitWrite(path, length)) return ResponseEntity.ok();
        else return  ResponseEntity.fail("提交写出错");
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     * @param path
     * @return
     */
    @RequestMapping("open")
    public ResponseEntity open(@RequestParam String path) throws IOException {
        StatInfo fileStat = metaService.open(path);
        if (fileStat != null) return ResponseEntity.ok().data(fileStat);
        else return ResponseEntity.fail("文件打开失败");
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }

}
