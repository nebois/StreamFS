package com.nebois.streamfs.metaserver.domain;

import lombok.Data;

import java.util.List;

@Data
public class StatInfo
{
    public String path;
    public long size;
    public long mtime; // 修改时间
    public FileType type;
    private List<ReplicaData> replicaData;
}
