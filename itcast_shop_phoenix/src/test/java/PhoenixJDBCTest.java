import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/21 9:52
 */
public class PhoenixJDBCTest {

    public static void main(String[] args) throws Exception {
        /**
         * Phoenix JDBC实现步骤：
         * 1.注册驱动
         * 2.建立连接
         * 3.创建PrepareStatement
         * 4.执行sql语句
         * 5.遍历查询结果
         * 6.关闭连接
         */
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        Connection connection = DriverManager.getConnection("jdbc:phoenix:wbbigdata00:2181", "", "");

        String sql = "select * from \"dwd_order_detail\"";
        PreparedStatement pstmt = connection.prepareStatement(sql);

        ResultSet resultSet = pstmt.executeQuery();
        ArrayList<String> list = new ArrayList<String>();

        while (resultSet.next()){
            String rowid = resultSet.getString("rowid");
            String goodsThirdCatName = resultSet.getString("goodsThirdCatName");

            list.add("rowid: " + rowid + ", goodsThirdCatName " + goodsThirdCatName );

        }
        for (String s : list) {
            System.out.println(s);
        }
    }
}
