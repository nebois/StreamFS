package com.ksyun.campus.dataserver.domain.dto;

import lombok.Data;

@Data
public class ByteArrayDTO {
    private Boolean isEndOfFile = false;
    private String byteData;
    private Long fileSize;
}
