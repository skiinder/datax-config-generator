package com.atguigu.datax.configuration;

public class Configuration {
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String MYSQL_HOST = "hadoop102";
    public static final String MYSQL_PORT = "3306";
    public static final String MYSQL_DATABASE = "metastore";
    public static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
    public static final String[] MYSQL_TABLES = new String[]{};
    public static final String HDFS_URI = "hdfs://hadoop102:8020";
    public static final String OUT_DIR = "d:/output";
}
