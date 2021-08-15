import manager.MyManager;
import connector.MyConnector;
import org.apache.log4j.Logger;
import utils.Contants;
import utils.ReadPeoperties;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * description: test <br>
 * date: 2021/8/15 <br>
 * author: zz <br>
 * version: 1.0 <br>
 */
public class DataxTool {

    private static Logger logger = Logger.getLogger(DataxTool.class);

    private static ReadPeoperties readPeoperties = new ReadPeoperties();
    private static Properties props = new Properties();
    private static String showCreateTable = "SHOW CREATE TABLE `%s`";
    private static String tableComment = "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_NAME='%s'";
    private static String dbPropertiesStatic = MyManager.class.getClassLoader().getResource("DBInfo.properties").getPath();
    private static String jsonFileTempleStatic = MyManager.class.getClassLoader().getResource("dataxJsonTemple.json").getPath();


    public static void main(String[] args) throws IOException {

        if (args.length != 0) {
            dbPropertiesStatic = args[0];
            jsonFileTempleStatic = args[1];
        }
        MyConnector mysqlConnector = null;
        MyConnector hiveConnector = null;

        FileWriter stgFileWriter = null;
        FileWriter odsFileWriter = null;
        FileWriter mysqlFileWriter = null;
        try {
            MyManager manager = new MyManager();
            props = readPeoperties.getProperties(dbPropertiesStatic);
            manager.initConnector(props);
            mysqlConnector = manager.getMysqlConnector();
            hiveConnector = manager.getHiveConnector();

            boolean createTable = Boolean.valueOf(props.getProperty(Contants.CREATE_TABLE));
            String mysqlUrl = props.getProperty(Contants.MYSQL_URL);
            String db = mysqlUrl.substring(mysqlUrl.lastIndexOf("/") + 1);


            String mysqlTables = props.getProperty(Contants.MYSQL_TABLES);
            String[] tables = mysqlTables.split("\\|");

            stgFileWriter = new FileWriter("./" + db + "_stg.sql");
            odsFileWriter = new FileWriter("./" + db + "_ods.sql");
            mysqlFileWriter = new FileWriter("./" + db + "_mysql.sql");

            for (String tableInfo : tables) {
                String[] tb = tableInfo.split(":");
                String tableName = tb[0];
                String syncType = tb[1];


                List<ArrayList<String>> mysqlableCol = mysqlConnector.getMetaData(tableName);
                String createTableInfo = mysqlConnector.execQuery(String.format(showCreateTable, tableName));
                String tableCommentInfo = mysqlConnector.execQuery(String.format(tableComment, tableName));
                mysqlFileWriter.write(createTableInfo + ";\n");

                HashMap<String, String> map = manager.convertCreateTable(tableName, mysqlableCol, syncType, props, tableCommentInfo);
                String stg = map.get("stg");
                String ods = map.get("ods");


                stgFileWriter.write(stg + ";\n");
                odsFileWriter.write(ods + ";\n");
                System.out.println("table name:" + tableName + ",\twrite sql in stg and ods's file successfully!");

                manager.writeDataXJson(props, tableName, stg, syncType.equalsIgnoreCase("full"), jsonFileTempleStatic, db);
                System.out.println("table name:" + tableName + ",\twrite json file successfully!");

                if (createTable) {
                    hiveConnector.exec(stg);
                    System.out.println("table name:" + tableName + ",\tcreate table sql in stg successfully!");
                    hiveConnector.exec(ods);
                    System.out.println("table name:" + tableName + ",\tcreate table sql in ods successfully!");
                }


            }

        } catch (Exception e) {
            logger.error(e);
        } finally {
            try {
                mysqlFileWriter.close();
                stgFileWriter.close();
                odsFileWriter.close();
                mysqlConnector.destory();
                hiveConnector.destory();
            } catch (SQLException e) {
                logger.error(e);
            }
        }


    }

}
