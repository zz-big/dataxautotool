package connector;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/**
 * description: test <br>
 * date: 2021/8/15 <br>
 * author: zz <br>
 * version: 1.0 <br>
 */
public class MyConnector {


    private Connection con = null;
    private Statement state = null;
    private ResultSet res = null;
    private DatabaseMetaData metaData = null;


    public MyConnector(String driverName, String url, String user, String password) throws ClassNotFoundException, SQLException {

        Class.forName(driverName);
        con = DriverManager.getConnection(url, user, password);
        state = con.createStatement();
    }

    //创建数据库

    public void CreateDb() throws SQLException {

        state.execute("create database test");

    }

    // 查询所有数据库

    public void showtDb() throws SQLException {
        res = state.executeQuery("show databases");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }



    public boolean exec(String sql) throws SQLException {
        boolean execute = state.execute(sql);
        return execute;
    }

    public String execQuery(String sql) throws SQLException {
        ResultSet resultSet = state.executeQuery(sql);
        String reseult ="";
        ResultSetMetaData resultMetaData = resultSet.getMetaData();
        Integer colNum = resultMetaData.getColumnCount();
        while (resultSet.next()) {
            for (int j = 1; j <= colNum; j++) {
                reseult+=resultSet.getString(j) + "\t";
            }
        }
        return reseult;
    }



    public List<ArrayList<String>> getMetaData(String tableName) throws Exception {

        List<ArrayList<String>> tableMeta = new ArrayList<ArrayList<String>>();
        metaData = con.getMetaData();

        String columnName;
        String columnType;
        String comment;
        ResultSet colRet = state.executeQuery("show full columns from " + tableName);
        while (colRet.next()) {
            columnName = colRet.getString("Field");
            columnType = colRet.getString("Type");
            comment = colRet.getString("Comment");
//            System.out.println(columnName + " " + columnType + " " + comment);
            ArrayList<String> info = new ArrayList<String>();
            info.add(columnName);
            info.add(columnType);
            info.add(comment);
            tableMeta.add(info);
        }

        return tableMeta;
    }





    // 释放资源
    public void destory() throws SQLException {
        if (res != null) {
            state.close();
        }
        if (state != null) {
            state.close();
        }
        if (con != null) {
            con.close();
        }
    }
}
