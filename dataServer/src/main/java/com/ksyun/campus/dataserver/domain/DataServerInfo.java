package com.ksyun.campus.dataserver.domain;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;

@Data
public class DataServerInfo {
    private String host;
    private Integer port;
    private Long fileTotal;
    private Long capacity;
    private Long useCapacity;
}
