package com.nebois.streamfs.client.domain;

import lombok.Data;

@Data
public class ReplicaData {
    public String id;
    public String dsNode;//格式为ip:port
    public String path;

}
