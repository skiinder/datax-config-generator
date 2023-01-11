package com.atguigu.datax.configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Configuration {
    public static String MYSQL_USER;
    public static String MYSQL_PASSWORD;
    public static String MYSQL_HOST;
    public static String MYSQL_PORT;
    public static String MYSQL_DATABASE;
    public static String MYSQL_URL;
    public static String MYSQL_TABLES;
    public static String HDFS_URI;
    public static String OUT_DIR;

    static {
        Path path = Paths.get("configuration.properties");
        Properties configuration = new Properties();
        try {
            configuration.load(Files.newBufferedReader(path));
            MYSQL_USER = configuration.getProperty("mysql.username", "root");
            MYSQL_PASSWORD = configuration.getProperty("mysql.password", "000000");
            MYSQL_HOST = configuration.getProperty("mysql.host", "hadoop102");
            MYSQL_PORT = configuration.getProperty("mysql.port", "3306");
            MYSQL_DATABASE = configuration.getProperty("mysql.database", "mysql");
            MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES = configuration.getProperty("mysql.tables", "");
            HDFS_URI = configuration.getProperty("hdfs.uri", "hdfs://hadoop102:8020");
            OUT_DIR = configuration.getProperty("outdir", "d:/output");
        } catch (IOException e) {
            MYSQL_USER = "root";
            MYSQL_PASSWORD = "000000";
            MYSQL_HOST = "hadoop102";
            MYSQL_PORT = "3306";
            MYSQL_DATABASE = "metastore";
            MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES = "";
            HDFS_URI = "hdfs://hadoop102:8020";
            OUT_DIR = "d:/output";
        }
    }
}
