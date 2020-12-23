package com.itcast.test;

import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/14 13:17
 */
public class HttpdLogParsingDemo {

    public static String getLogFormat(){
        return "%u %h %l %t \"%r\" %>s \"%{Referer}i\" \"%{User-Agent}i\"";
    }

    public static String getInputLine(){
        return "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; N1-N1) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\"";
    }

    public static void run() throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {
        HttpdLoglineParser<HttpdLogRecord> parser = new HttpdLoglineParser<>(HttpdLogRecord.class, getLogFormat());
        parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user");
        parser.addParseTarget("setIp", "IP:connection.client.host");
        parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method");
        parser.addParseTarget("setRequestStatus", "STRING:request.status.last");

        Main main = new Main();
        main.printAllPossibles(getLogFormat());
    }

    public static void main(String[] args) throws NoSuchMethodException, InvalidDissectorException, MissingDissectorsException {
        run();
    }
}
