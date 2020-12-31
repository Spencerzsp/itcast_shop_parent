package com.itcast.canal.enums;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/29 14:43
 */
public enum TravelEnum {

    FOLLOW_TEAM("01", "follow", "跟团"),
    PRIVATE("02", "follow", "私家"),
    SELF_HELP("03", "self_help", "半自助");

    private String code;
    private String desc;
    private String remark;

    TravelEnum(String code, String desc, String remark) {
        this.code = code;
        this.desc = desc;
        this.remark = remark;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public static void main(String[] args) {
        System.out.println(TravelEnum.FOLLOW_TEAM.desc);
    }
}
