package com.bigdata.itcast.mapper;

import com.bigdata.itcast.pojo.CartPojo;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @ description: 使用phoenix JDBC整合mybatis查询HBase数据
 * @ author: spencer
 * @ date: 2020/12/24 10:57
 */
@Mapper
public interface CartMapper {

    /**
     * 根据goodsId查询phoenix中的数据
     * @param goodsId
     * @return
     */
    CartPojo getCartDataByGoodsId(String goodsId);

    /**
     * 查询phoenix全部数据
     * @return
     */
    List<CartPojo> getCartData();

    /**
     * 查询具体的userId相同的次数
     * @param userId
     * @return
     */
    int getCartCountsByUserId(String userId);

    /**
     * 查询数据条数
     * @return
     */
    int getCartCounts();
}
