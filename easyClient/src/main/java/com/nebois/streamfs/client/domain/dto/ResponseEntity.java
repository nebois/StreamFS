package com.nebois.streamfs.client.domain.dto;


import lombok.Data;

@Data
public  class ResponseEntity<T> {

    private int code;

    private String desc;

    private T data;

    public static ResponseEntity ok(){
        ResponseEntity responseEntity = new ResponseEntity();
        responseEntity.setCode(200);
        responseEntity.setDesc("oooooook");
        return responseEntity;
    }

    public static ResponseEntity fail(){
        ResponseEntity responseEntity = new ResponseEntity();
        responseEntity.setCode(500);
        responseEntity.setDesc("oops!");
        return responseEntity;
    }

    public static ResponseEntity fail(String desc) {
        return ResponseEntity.fail().desc(desc);
    }

    public ResponseEntity data(T data) {
        this.data = data;
        return this;
    }
    public ResponseEntity desc(String desc) {
        this.desc = desc;
        return this;
    }
}
