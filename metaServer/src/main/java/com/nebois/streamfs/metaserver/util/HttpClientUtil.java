package com.nebois.streamfs.metaserver.util;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class HttpClientUtil {
    private static HttpClient httpClient;

    public static String httpPostData(String urlWithParam, byte[] data) throws IOException {
        HttpClient httpClient = HttpClientUtil.createHttpClient(new HttpClientConfig());
        HttpPost httpPost = new HttpPost(urlWithParam);

        ByteArrayEntity byteArrayEntity = new ByteArrayEntity(data, ContentType.APPLICATION_JSON);
        httpPost.setEntity(byteArrayEntity);

        CloseableHttpResponse response = (CloseableHttpResponse) httpClient.execute(httpPost);
        InputStream content = response.getEntity().getContent();
        String responseJson = readAll(content);
        return responseJson;
    }

    public static String httpGet(String urlWithParam) throws IOException {
        HttpClient httpClient = HttpClientUtil.createHttpClient(new HttpClientConfig());
        HttpGet get = new HttpGet(urlWithParam);
        CloseableHttpResponse response = (CloseableHttpResponse) httpClient.execute(get);
        InputStream content = response.getEntity().getContent();
        String responseJson = readAll(content);
        return responseJson;
    }

    public static String httpGet(String host, Integer port, String api, String params) throws IOException {
        String urlWithParam = "http://" + host + ":" + port + api + "?" + params;
        return httpGet(urlWithParam);
    }

    public static String readAll(InputStream content) throws IOException {
        byte[] buff = new byte[255];
        int len;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        while ((len = content.read(buff)) != -1) {
            outputStream.write(buff, 0, len);
        }
        return outputStream.toString();
    }
    public static HttpClient createHttpClient(HttpClientConfig config) {

        int socketSendBufferSizeHint = config.getSocketSendBufferSizeHint();
        int socketReceiveBufferSizeHint = config.getSocketReceiveBufferSizeHint();
        int buffersize = 0;
        if (socketSendBufferSizeHint > 0 || socketReceiveBufferSizeHint > 0) {
            buffersize = Math.max(socketSendBufferSizeHint, socketReceiveBufferSizeHint);
        }
        SocketConfig soConfig = SocketConfig.custom()
                .setTcpNoDelay(true).setSndBufSize(buffersize)
                .setSoTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                .build();
        ConnectionConfig coConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .build();
        RequestConfig reConfig;
        RequestConfig.Builder builder= RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .setResponseTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                ;
        reConfig=builder.build();
        PlainConnectionSocketFactory sf = PlainConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory> create().register("http", sf).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(r);
        connectionManager.setMaxTotal(config.getMaxConnections());
        connectionManager.setDefaultMaxPerRoute(connectionManager.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(coConfig);
        connectionManager.setDefaultSocketConfig(soConfig);


        httpClient = HttpClients.custom().setConnectionManager(connectionManager).setRetryStrategy(new DefaultHttpRequestRetryStrategy(config.getMaxRetry(), TimeValue.ZERO_MILLISECONDS))
                .setDefaultRequestConfig(reConfig)
                .build();
        return httpClient;
    }
}
