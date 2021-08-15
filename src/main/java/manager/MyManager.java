package manager;

import connector.MyConnector;
import org.apache.log4j.Logger;
import utils.Contants;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * description: test <br>
 * date: 2021/8/15 <br>
 * author: zz <br>
 * version: 1.0 <br>
 */
public class MyManager {

    private static Logger logger = Logger.getLogger(MyManager.class);

    private MyConnector mysqlConnector;
    private MyConnector hiveConnector;

    public MyConnector getMysqlConnector() {
        return mysqlConnector;
    }

    public MyConnector getHiveConnector() {
        return hiveConnector;
    }

    public void initConnector(Properties props) throws SQLException, ClassNotFoundException {
        String mySqlDriverName = props.getProperty(Contants.MYSQL_DRIVER_NAME);
        String mySqlurl = props.getProperty(Contants.MYSQL_URL);
        String mySqluser = props.getProperty(Contants.MYSQL_USER);
        String mySqlpassword = props.getProperty(Contants.MYSQL_PASSWORD);
        String hiveDriverName = props.getProperty(Contants.HIVE_DRIVER_NAME);
        String hiveUrl = props.getProperty(Contants.HIVE_URL);
        String hiveUser = props.getProperty(Contants.HIVE_USER);
        String hivePassword = props.getProperty(Contants.HIVE_PASSWORD);
        mysqlConnector = new MyConnector(mySqlDriverName, mySqlurl, mySqluser, mySqlpassword);
        hiveConnector = new MyConnector(hiveDriverName, hiveUrl, hiveUser, hivePassword);
    }

    public HashMap<String, String> convertCreateTable(String tableName, List<ArrayList<String>> tableInfo, String syncType, Properties props, String tableComment) {
        HashMap<String, String> map = new HashMap<String, String>();
        String stgTableName = tableName + "_" + props.getProperty(Contants.HIVE_STG_TABLE_LAST_FIX);
        String odsTableName = tableName + "_" + props.getProperty(Contants.HIVE_ODS_TABLE_LAST_FIX);
        String convertCreateTableToSTG = convertCreateTableToSTG(stgTableName, tableInfo, syncType, props, tableComment);
        String convertCreateTableToOds = convertCreateTableToOds(odsTableName, tableInfo, syncType, props, tableComment);

        map.put("stg", convertCreateTableToSTG);
        map.put("ods", convertCreateTableToOds);
        return map;
    }

    private String convertCreateTableToSTG(String tableName, List<ArrayList<String>> tableInfo, String syncType, Properties props, String tableComment) {
        Iterator<ArrayList<String>> it = tableInfo.iterator();
        String colString = "";

        while (it.hasNext()) {
            ArrayList<String> info = it.next();
            String col = info.get(0);
            String col_type = info.get(1);
            String comment = info.get(2);

            String hive_type = "";
            if (col_type.startsWith(Contants.MYSQL_INT) || col_type.startsWith(Contants.MYSQL_SMALLINT) || col_type.startsWith(Contants.MYSQL_MEDIUMINT)) {
                hive_type = Contants.HIVE_INT;
            } else if (col_type.startsWith(Contants.MYSQL_VARCHAR) || col_type.startsWith(Contants.MYSQL_CHAR)) {
                hive_type = col_type;
            } else if (col_type.startsWith(Contants.MYSQL_TINYINT)) {
                hive_type = Contants.HIVE_TINYINT;
            } else if (col_type.startsWith(Contants.MYSQL_DOUBLE)) {
                hive_type = Contants.HIVE_DOUBLE;
            } else if (col_type.startsWith(Contants.MYSQL_DECIMAL)) {
                hive_type = Contants.HIVE_STRING;
            } else if (col_type.startsWith(Contants.MYSQL_FLOAT)) {
                hive_type = Contants.HIVE_FLOAT;
            } else {
                hive_type = Contants.HIVE_STRING;
            }

            colString += " `" + col + "` " + hive_type + " COMMENT\"" + comment + "\",\n";

        }

        String external = props.getProperty(Contants.HIVE_STG_TABLE_TYPE);
        String hiveTablePath = props.getProperty(Contants.HIVE_STG_TABLE_PATH);
        String hiveStgTableprefix = props.getProperty(Contants.HIVE_STG_TABLE_PREFIX);
        String hiveStgRowFormat = props.getProperty(Contants.HIVE_STG_ROW_FORMAT);
        String hiveStgStoreType = props.getProperty(Contants.HIVE_STG_STORE_TYPE);
        String hiveStgCompressionType = props.getProperty(Contants.HIVE_STG_COMPRESSION_TYPE);

        String creataTableString = createTableString(colString, tableName, syncType, external, hiveTablePath, hiveStgTableprefix, hiveStgRowFormat, hiveStgStoreType, hiveStgCompressionType, tableComment);


        return creataTableString;
    }

    private String convertCreateTableToOds(String tableName, List<ArrayList<String>> tableInfo, String syncType, Properties props, String tableComment) {
        Iterator<ArrayList<String>> it = tableInfo.iterator();
        String colString = "";

        while (it.hasNext()) {
            ArrayList<String> info = it.next();
            String col = info.get(0);
            String col_type = info.get(1);
            String comment = info.get(2);

            String hive_type = "";
            if (col_type.startsWith(Contants.MYSQL_INT) || col_type.startsWith(Contants.MYSQL_SMALLINT) || col_type.startsWith(Contants.MYSQL_MEDIUMINT)) {
                hive_type = Contants.HIVE_INT;
            } else if (col_type.startsWith(Contants.MYSQL_VARCHAR) || col_type.startsWith(Contants.MYSQL_CHAR)) {
                hive_type = col_type;
            } else if (col_type.startsWith(Contants.MYSQL_TINYINT)) {
                hive_type = Contants.HIVE_TINYINT;
            } else if (col_type.startsWith(Contants.MYSQL_DOUBLE)) {
                hive_type = col_type;
            } else if (col_type.startsWith(Contants.MYSQL_DECIMAL)) {
                hive_type = col_type;
            } else if (col_type.startsWith(Contants.MYSQL_FLOAT)) {
                hive_type = col_type;
            } else {
                hive_type = Contants.HIVE_STRING;
            }

            colString += " `" + col + "` " + hive_type + " COMMENT \"" + comment + "\",\n";

        }

        String external = props.getProperty(Contants.HIVE_ODS_TABLE_TYPE);
        String hiveOdsTablePath = props.getProperty(Contants.HIVE_ODS_TABLE_PATH);
        String hiveOdsTableprefix = props.getProperty(Contants.HIVE_ODS_TABLE_PREFIX);
        String hiveOdsRowFormat = props.getProperty(Contants.HIVE_ODS_ROW_FORMAT);
        String hiveOdsStoreType = props.getProperty(Contants.HIVE_ODS_STORE_TYPE);
        String hiveOdsCompressionType = props.getProperty(Contants.HIVE_ODS_COMPRESSION_TYPE);

        String creataTableString = createTableString(colString, tableName, syncType, external, hiveOdsTablePath, hiveOdsTableprefix, hiveOdsRowFormat, hiveOdsStoreType, hiveOdsCompressionType, tableComment);


        return creataTableString;
    }

    public void writeDataXJson(Properties props, String tableName, String stg, boolean isFull, String jsonFileTemple, String db) {

        BufferedReader bufferedReader = null;
        StringBuilder jsonStringBuilder = new StringBuilder();
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(jsonFileTemple)), "utf-8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                jsonStringBuilder.append(line + "\n");
            }
        } catch (UnsupportedEncodingException e) {
            logger.error(e);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }

        String json = jsonStringBuilder.toString();
        json = json.replace("$username", props.getProperty(Contants.MYSQL_USER));
        json = json.replace("$password", props.getProperty(Contants.MYSQL_PASSWORD));
        json = json.replace("$table", tableName);
        json = json.replace("$jdbcUrl", props.getProperty(Contants.MYSQL_URL));
        json = json.replace("$defaultFS", props.getProperty(Contants.DEFAULT_FS));
        json = json.replace("$fileType", props.getProperty(Contants.HIVE_STG_STORE_TYPE));
        json = json.replace("$path", props.getProperty(Contants.HIVE_STG_TABLE_PATH) + "/" + tableName);

        String column = "";

        String[] cols = stg.substring(stg.indexOf("(\n") + 1, stg.indexOf("\n)")).split(",");

        for (String col : cols) {
            String[] colInfos = col.split(" ");
            String colName = colInfos[1].replaceAll("`", "");
            String colType = colInfos[2];
            column += "              {\n               \"name\": \"" + colName + "\",\n" +
                    "               \"type\": \"" + colType + "\"\n              },\n";

        }

        json = json.replace("\"$column\"", column.substring(0, column.lastIndexOf(",")) + "\n");
        json = json.replace("$writeMode", props.getProperty(Contants.HIVE_STG_STORE_TYPE));
        json = json.replace("\"$fieldDelimiter\"", props.getProperty(Contants.HIVE_STG_ROW_FORMAT));
        json = json.replace("$compress", props.getProperty(Contants.HIVE_STG_COMPRESSION_TYPE));

        if (isFull) {
            json = json.replace("$col", "*");
            json = json.replace("$where", " where 1 = 1 ");


        } else {
            json = json.replace("$col", " *, " + props.getProperty(Contants.HIVE_STG_QUERY_SQL_PARTATION));
            json = json.replace("$where", props.getProperty(Contants.HIVE_STG_QUERY_SQL_PARTATION_WHERE));
        }

        FileWriter jsonFileWriter = null;
        try {
            File filePath = new File("./dataxJson/");

            //如果目录不存在,则创建目录;
            if (!filePath.exists()) {
                //mkdirs是创建多级目录,而mkdir是创建单级目录;
                filePath.mkdirs();
            }
            jsonFileWriter = new FileWriter(filePath.getPath() + "/" + db + "_" + tableName + "_datax.json");
            jsonFileWriter.write(json);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            try {
                jsonFileWriter.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }


    private String createTableString(String colString, String tableName, String syncType, String external, String hiveTablePath, String hiveTableprefix, String hiveRowFormat, String hiveStoreType, String hiveCompressionType, String tableComment) {
        String db = "";
        String externalString = "";
        String location = "";
        String creataTableString = "";


        if (hiveTablePath.endsWith("/")) {
            db = hiveTablePath.substring(hiveTablePath.lastIndexOf("/") + 1, hiveTablePath.length());
        } else {
            db = hiveTablePath.substring(hiveTablePath.lastIndexOf("/") + 1);
        }
        if (db.contains(".")) {
            db = db.substring(0, db.indexOf("."));
        }
        if (external.equalsIgnoreCase(Contants.HIVE_EXTERNAL)) {
            externalString = Contants.HIVE_EXTERNAL;
            location = "LOCATION " + hiveTablePath;
        }

        if (!hiveStoreType.equalsIgnoreCase(Contants.HIVE_STGSTORE_TEXT_TYPE)) {
            hiveStoreType += String.format(" tblproperties " + " (\"%s.compress\"=\"%s\")", hiveStoreType.toLowerCase(), hiveCompressionType);
        }
        if (!syncType.equalsIgnoreCase(Contants.SYNC_TYPE_FULL)) {
            syncType = String.format("PARTITIONED BY (%s)\n", syncType);
        } else {
            syncType = "";
        }


        if (!Pattern.matches("[ \\f\\r\\t\\n]", tableComment)) {
            tableComment = "COMMENT \"" + tableComment + "\"";
        } else {
            tableComment = "";
        }

        creataTableString = String.format("create %s table  IF NOT EXISTS %s.%s_%s (\n%s\n) %s\n%s" +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY %s \n" +
                        "STORED AS %s\n",
                externalString, db, hiveTableprefix, tableName,
                colString.substring(0, colString.lastIndexOf(",")), tableComment, syncType, hiveRowFormat, hiveStoreType, location);

        //        System.out.println(creataTableString);
        return creataTableString;

    }

}
