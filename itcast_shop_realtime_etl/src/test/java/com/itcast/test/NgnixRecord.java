package com.itcast.test;

import lombok.Getter;
import lombok.Setter;
import nl.basjes.parse.core.Field;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/15 11:11
 */
public class NgnixRecord {

    @Setter @Getter private String ip;
    @Setter @Getter private String requestTime;
    @Setter @Getter private String method;
    @Setter @Getter private String requestStatus;
    @Setter @Getter private String responseBodyBytes;
    @Setter @Getter private String userAgent;

    private final Map<String, String> results = new HashMap<>(32);

    public NgnixRecord() {

    }

    public NgnixRecord(String ip, String requestTime, String method, String requestStatus, String responseBodyBytes, String userAgent) {
        this.ip = ip;
        this.requestTime = requestTime;
        this.method = method;
        this.requestStatus = requestStatus;
        this.responseBodyBytes = responseBodyBytes;
        this.userAgent = userAgent;
    }

    @Field("IP:connection.client.host")
    public void setIp(final String name, final String value){
        results.put(name, value);
    }


    @Field("TIME.STAMP:request.receive.time.last")
    public void setRequestTime(final String name, final String value){
        results.put(name, value);
    }
    @Field("HTTP.METHOD:request.firstline.method")
    public void setMethod(final String name, final String value){
        results.put(name, value);
    }

    @Field("STRING:request.status.last")
    public void setRequestStatus(final String name, final String value){
        results.put(name, value);
    }

    @Field("BYTES:response.body.bytes")
    public void setResponseBodyBytes(final String name, final String value){
        results.put(name, value);
    }

//    @Field("HTTP.URI:request.referer")
//    public void setReferer(final String name, final String value){
//        results.put(name, value);
//    }

    @Field("HTTP.USERAGENT:request.user-agent")
    public void setUserAgent(final String name, final String value){
        results.put(name, value);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        TreeSet<String> keys = new TreeSet<>(results.keySet());
        NgnixRecord ngnixRecord = new NgnixRecord();
        for (String key : keys) {
            sb.append(key).append(" = ").append(results.get(key)).append('\n');
            ngnixRecord.setIp(results.get("IP:connection.client.host"));
            ngnixRecord.setRequestTime(results.get("TIME.STAMP:request.receive.time.last"));
            System.out.println(ngnixRecord.ip);
//            ngnixRecord.setIp(results.get("IP:connection.client.host"));
//            ngnixRecord.setIp(results.get("IP:connection.client.host"));
//            ngnixRecord.setIp(results.get("IP:connection.client.host"));
//            ngnixRecord.setIp(results.get("IP:connection.client.host"));
        }

        return sb.toString();
    }

    public void clear() {
        results.clear();
    }
}
