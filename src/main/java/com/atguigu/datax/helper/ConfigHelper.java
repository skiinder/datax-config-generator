package com.atguigu.datax.helper;

import java.io.IOException;
import java.util.Properties;

public class ConfigHelper {
    public final String MYSQL_USER;
    public final String MYSQL_PASSWORD;
    public final String MYSQL_HOST;
    public final String MYSQL_PORT;
    public final String MYSQL_DATABASE;
    public final String MYSQL_TABLES;
    public final String HDFS_URI;
    public final String OUT_DIR;


    public ConfigHelper() {

        Properties config = getConfig();
        MYSQL_USER = config.getProperty("mysql.user");
        MYSQL_PASSWORD = config.getProperty("mysql.password");
        MYSQL_HOST = config.getProperty("mysql.host");
        MYSQL_PORT = config.getProperty("mysql.port");
        MYSQL_DATABASE = config.getProperty("mysql.database");
        MYSQL_TABLES = config.getProperty("mysql.tables");
        HDFS_URI = config.getProperty("hdfs.uri");
        OUT_DIR = config.getProperty("outdir");

    }

    private Properties getConfig() {
        Properties properties = new Properties();
        try {
            properties.load(ConfigHelper.class.getResourceAsStream("/configuration.properties"));
        } catch (IOException e) {
            System.out.println("配置文件加载失败！");
        }
        return properties;
    }
}
