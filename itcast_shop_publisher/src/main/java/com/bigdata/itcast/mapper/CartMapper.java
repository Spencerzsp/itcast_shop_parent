package com.bigdata.itcast.mapper;

import com.bigdata.itcast.pojo.CartPojo;
import org.apache.ibatis.annotations.Mapper;

/**
 * @ description:
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
}
