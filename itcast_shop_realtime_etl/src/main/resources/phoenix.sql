create view if not exists "dwd_order_detail"(
	"rowid" varchar primary key,
	"detail"."ogId" VARCHAR ,
	"detail"."orderId" VARCHAR ,
	"detail"."goodsId" VARCHAR ,
	"detail"."goodsNum" VARCHAR ,
	"detail"."goodsPrice" VARCHAR ,
	"detail"."goodsName" VARCHAR ,
	"detail"."shopId" VARCHAR ,
	"detail"."goodsThirdCatId" VARCHAR ,
	"detail"."goodsThirdCatName" VARCHAR ,
	"detail"."goodsSecondCatId" VARCHAR ,
	"detail"."goodsSecondCatName" VARCHAR ,
	"detail"."goodsFirstCatId" VARCHAR ,
	"detail"."goodsFirstCatName" VARCHAR ,
	"detail"."areaId" VARCHAR ,
	"detail"."shopName" VARCHAR ,
	"detail"."shopCompany" VARCHAR ,
	"detail"."cityId" VARCHAR ,
	"detail"."cityName" VARCHAR ,
	"detail"."regionId" VARCHAR ,
	"detail"."regionName" VARCHAR
)

-- 创建多个字段的二级索引
CREATE LOCAL index "idx_dwd_order_detail" ON "dwd_order_detail"(
"detail"."goodsThirdCatName",
"detail"."goodsSecondCatName",
"detail"."goodsFirstCatName",
"detail"."cityName",
"detail"."regionName"
);

CREATE VIEW if NOT EXISTS "dwd_itcast_cart"(
  "rowid" VARCHAR NOT NULL PRIMARY KEY ,
  "cart"."goodsId" VARCHAR ,
  "cart"."userId" VARCHAR ,
  "cart"."count" VARCHAR ,
  "cart"."guid" VARCHAR ,
  "cart"."addTime" VARCHAR ,
  "cart"."ip" VARCHAR ,
  "cart"."goodsPrice" VARCHAR ,
  "cart"."goodsName" VARCHAR ,
  "cart"."goodsCat3" VARCHAR ,
  "cart"."goodsCat2" VARCHAR ,
  "cart"."goodsCat1" VARCHAR ,
  "cart"."shopId" VARCHAR ,
  "cart"."shopName" VARCHAR ,
  "cart"."shopProvinceId" VARCHAR ,
  "cart"."shopProvinceName" VARCHAR ,
  "cart"."shopCityId" VARCHAR ,
  "cart"."shopCityName" VARCHAR ,
  "cart"."clientProvince" VARCHAR ,
  "cart"."clientCity" VARCHAR
)