package com.ksyun.campus.dataserver.domain;

import lombok.Data;

@Data
public class DirectoryStats {
    private Long fileTotal;
    private Long useCapacity;

    public void addFileTotal(int cnt) {
        this.fileTotal += cnt;
    }

    public void addUseCapacity(long cnt) {
        this.useCapacity += cnt;
    }

    public DirectoryStats(long ft, long uc) {
        this.fileTotal = ft;
        this.useCapacity = uc;
    }
}
