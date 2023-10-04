package com.ksyun.campus.client.domain;

import lombok.Data;

@Data
public class DataServerInfo {
    private String host;
    private Integer port;
    private Long fileTotal;
    private Long capacity;
    private Long useCapacity;
}
