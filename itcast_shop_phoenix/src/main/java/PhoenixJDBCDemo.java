import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ description: 测试phoenix JDBC查询habse数据
 * @ author: spencer
 * @ date: 2020/12/18 16:15
 */
public class PhoenixJDBCDemo {

    public static void main(String[] args) throws Exception {
        /**
         * 1.注册驱动
         * 2.创建连接
         * 3.创建prepareStatement
         * 4.执行sql查询
         * 5.循环遍历
         * 6.关闭连接
         */
        // 1.注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        // 2.创建连接
        String zks = "jdbc:phoenix:wbbigdata00,wbbigdata01,wbbigdata02:2181";
        Connection connection = DriverManager.getConnection(zks, "", "");

        // 3.创建prepareStatement
        String sql = "select * from \"dwd_order_detail\"";
        PreparedStatement pstmt = connection.prepareStatement(sql);

        // 4.执行sql查询
        ResultSet resultSet = pstmt.executeQuery();
        
        // 5.循环遍历
        while (resultSet.next()){
            String rowid = resultSet.getString("rowid");
            String goodsThirdCatName = resultSet.getString("goodsThirdCatName");

            System.out.println(rowid + ": "+ goodsThirdCatName);
        }

        // 6.关闭连接
        resultSet.close();
        pstmt.close();
        connection.close();
    }
}
