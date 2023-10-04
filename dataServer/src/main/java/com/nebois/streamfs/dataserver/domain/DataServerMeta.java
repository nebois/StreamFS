package com.nebois.streamfs.dataserver.domain;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Data
@Component
public class DataServerMeta {

    @Value("${server.port}")
    private Integer port;

    @Value("${capacity.maxCapacity}")
    private Long maxCapacity;

    @Value("${az.rack}")
    private String rack;

    @Value("${az.zone}")
    private String zone;
}
