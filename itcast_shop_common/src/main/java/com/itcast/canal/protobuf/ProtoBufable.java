package com.itcast.canal.protobuf;

/**
 * @ description: 定义一个protobuf的序列化接口
 *                这个接口返回的是二进制字节码对象
 *                所有能够使用protobuf序列化的bean都要实现该接口
 * @ author: spencer
 * @ date: 2020/12/9 16:05
 */
public interface ProtoBufable {
    /**
     * 将protobuf对象转换成二进制数组
     * @return
     */
    byte[] toBytes();
}
