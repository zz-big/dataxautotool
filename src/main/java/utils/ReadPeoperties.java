package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * description: test <br>
 * date: 2021/8/15 <br>
 * author: zz <br>
 * version: 1.0 <br>
 */
public class ReadPeoperties {


    /**
     * 配置文件路径
     */
    private String filePath;

    private Properties properties = new Properties();


    public void setFilePath(String path) {
        this.filePath = path;

    }

    public synchronized Properties getProperties(String filePath) throws IOException {

        //可以用两种不同的流来加载配置文件
        properties.load(new BufferedReader(new FileReader(filePath)));

//            properties.load(ReadPeoperties.class.getResourceAsStream(filePath));

        return properties;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
