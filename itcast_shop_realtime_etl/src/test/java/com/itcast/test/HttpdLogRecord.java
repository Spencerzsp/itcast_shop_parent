package com.itcast.test;

import lombok.Getter;
import lombok.Setter;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/14 16:42
 */
public class HttpdLogRecord {

    @Getter @Setter private String connectionClientUser = null;
    @Getter @Setter private String ip = null;
    @Getter @Setter private String method = null;
    @Getter @Setter private String requestStatus = null;
}
