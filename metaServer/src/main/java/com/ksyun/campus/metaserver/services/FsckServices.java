package com.ksyun.campus.metaserver.services;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class FsckServices {

    //@Scheduled(cron = "0 0 0 * * ?") // 每天 0 点执行
    @Scheduled(fixedRate = 30*60*1000) // 每隔 30 分钟执行一次
    public void fsckTask() {
    }
}
