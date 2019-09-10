package com.dhy.hive;

import org.apache.log4j.Logger;

import java.sql.*;


/**
 * @author dinghy
 * @date 2019/9/10 17:04
 */
public class HiveTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://hadoop101:10000/default";
    private static String user = "";
    private static String password = "";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveTest.class);

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, "hadoop", "123456");
        Statement stmt = conn.createStatement();
        String tableName = "test";

        //创建表
        stmt.execute("create table  if not exists " + tableName + " (id bigint, name string,age int)");
        //插入
        stmt.execute("insert into " +tableName + "( id ,name ,age ) values(1,'张三',100   )"  );
        stmt.execute("insert into " +tableName + "( id ,name ,age ) values(2,'李四',200   )"  );
        stmt.execute("insert into " +tableName + "( id ,name ,age ) values(3,'王五',300   )"  );

        sql = "select * from  " + tableName;
        res = stmt.executeQuery(sql);
        System.out.println(res);
        if (res.next()) {
            System.out.println(res.getLong(1) +"," + res.getString(2) + "," + res.getInt(3));
        }

        stmt.close();
        conn.close();
    }
}
