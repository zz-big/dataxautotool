1.编译
    mvn clean install

2.生成2个jar包，其中*with-dependencies.jar包含所有的依赖包
    dataxtool-1.0-SNAPSHOT.jar
    dataxtool-1.0-SNAPSHOT-jar-with-dependencies.jar

3.修改resources下的配置文件dataxJsonTemple.json，DBInfo.properties
    dataxJsonTemple.json datax json文件模板
    DBInfo.properties  mysql和hive的连接配置文件，hive需先创建stg和ods库

4.上传到服务器执行
    默认包含dataxJsonTemple.json和DBInfo.properties，可以自行调整后并指定文件路径运行，示例:
    java -jar  dataxtool-1.0-SNAPSHOT-jar-with-dependencies.jar  ./DBInfo.properties  ./dataxJsonTemple.json