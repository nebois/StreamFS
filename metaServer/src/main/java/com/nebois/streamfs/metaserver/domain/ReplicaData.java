package com.nebois.streamfs.metaserver.domain;

import lombok.Data;

@Data
public class ReplicaData {
    public String id;
    public String dsNode;
    public String path;

}
