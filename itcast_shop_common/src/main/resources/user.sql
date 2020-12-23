/*
Navicat MySQL Data Transfer

Source Server         : windows
Source Server Version : 50527
Source Host           : localhost:3306
Source Database       : userdb

Target Server Type    : MYSQL
Target Server Version : 50527
File Encoding         : 65001

Date: 2019-12-23 19:35:55
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `user`
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id` tinytext,
  `name` tinytext,
  `age` int(11) DEFAULT NULL,
  `gender` int(11) DEFAULT NULL,
  `province` tinytext,
  `city` tinytext,
  `region` tinytext,
  `phone` tinytext,
  `birthday` tinytext,
  `hobby` tinytext,
  `register_date` tinytext
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of user
-- ----------------------------
INSERT INTO `user` VALUES ('392456197008193000', '张三', '20', '0', '北京市', '昌平区', '嘻嘻哈哈', '18589407692', '1970-08-19', '美食;篮球;足球', '2018-08-06 09:44:43');
INSERT INTO `user` VALUES ('267456198006210000', '李四', '25', '1', '河南省', '郑州市', '郑东新区', '18681109672', '1980-06-21', '音乐;阅读;旅游', '2017-04-07 09:14:13');
INSERT INTO `user` VALUES ('892456199007203000', '王五', '24', '1', '湖北省', '武汉市', '汉阳区', '18798009102', '1990-07-20', '写代码;读代码;算法', '2016-06-08 07:34:23');
INSERT INTO `user` VALUES ('492456198712198000', '赵六', '26', '2', '陕西省', '西安市', '莲湖区', '18189189195', '1987-12-19', '购物;旅游', '2016-01-09 19:15:53');
INSERT INTO `user` VALUES ('392456197008193000', '张三', '20', '0', '北京市', '昌平区', '嘻嘻哈哈', '18589407692', '1970-08-19', '美食;篮球;足球', '2018-08-06 09:44:43');
INSERT INTO `user` VALUES ('392456197008193000', '张三', '20', '0', '北京市', '昌平区', '嘻嘻哈哈', '18589407692', '1970-08-19', '美食;篮球;足球', '2018-08-06 09:44:43');
