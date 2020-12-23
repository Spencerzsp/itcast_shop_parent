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