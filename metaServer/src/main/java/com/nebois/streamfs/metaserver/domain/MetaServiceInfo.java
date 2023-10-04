package com.nebois.streamfs.metaserver.domain;

import lombok.Data;

@Data
public class MetaServiceInfo {
    private String host;

    private Integer port;

    public MetaServiceInfo(String host) {
        this.host = host;
    }
}
