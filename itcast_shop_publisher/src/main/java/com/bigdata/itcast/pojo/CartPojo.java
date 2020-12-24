package com.bigdata.itcast.pojo;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 11:47
 */
public class CartPojo {

    private String rowid;
    private String goodsId;
    private String shopName;
    private String goodsName;

    public CartPojo() {
    }

    public CartPojo(String rowid, String goodsId, String shopName, String goodsName) {
        this.rowid = rowid;
        this.goodsId = goodsId;
        this.shopName = shopName;
        this.goodsName = goodsName;
    }

    @Override
    public String toString() {
        return "CartPojo{" +
                "rowid='" + rowid + '\'' +
                ", goodsId='" + goodsId + '\'' +
                ", shopName='" + shopName + '\'' +
                ", goodsName='" + goodsName + '\'' +
                '}';
    }

    public String getRowid() {
        return rowid;
    }

    public void setRowid(String rowid) {
        this.rowid = rowid;
    }

    public String getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(String goodsId) {
        this.goodsId = goodsId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }
}
