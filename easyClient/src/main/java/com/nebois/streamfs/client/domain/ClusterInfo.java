package com.nebois.streamfs.client.domain;

import lombok.Data;

import java.util.List;

@Data
public class ClusterInfo {
    private MetaServerInfo masterMetaServer;
    private MetaServerInfo slaveMetaServer;
    private List<DataServerInfo> dataServer;

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +
                ", slaveMetaServer=" + slaveMetaServer +
                ", dataServer=" + dataServer +
                '}';
    }
}
