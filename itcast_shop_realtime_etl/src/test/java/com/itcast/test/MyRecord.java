package com.itcast.test;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/11 13:42
 */
import nl.basjes.parse.core.Field;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class MyRecord {

    private final Map<String, String> results = new HashMap<>(32);

    @Field("STRING:request.firstline.uri.query.*")
    public void setQueryDeepMany(final String name, final String value) {
        results.put(name, value);
    }

    @Field("STRING:request.firstline.uri.query.img")
    public void setQueryImg(final String name, final String value) {
        results.put(name, value);
    }

    @Field("IP:connection.client.host")
    public void setIP(final String value) {
        results.put("IP:connection.client.host", value);
    }

    @Field({
            "HTTP.QUERYSTRING:request.firstline.uri.query",
            "NUMBER:connection.client.logname",
            "STRING:connection.client.user",
            "TIME.STAMP:request.receive.time",
            "HTTP.URI:request.firstline.uri",
            "BYTESCLF:response.body.bytes",
            "HTTP.URI:request.referer",
            "HTTP.USERAGENT:request.user-agent",
            "TIME.DAY:request.receive.time.day",
            "TIME.HOUR:request.receive.time.hour",
            "TIME.MONTHNAME:request.receive.time.monthname"
    })
    public void setValue(final String name, final String value) {
        results.put(name, value);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        TreeSet<String> keys = new TreeSet<>(results.keySet());
        for (String key : keys) {
            sb.append(key).append(" = ").append(results.get(key)).append('\n');
        }

        return sb.toString();
    }

    public void clear() {
        results.clear();
    }
}
