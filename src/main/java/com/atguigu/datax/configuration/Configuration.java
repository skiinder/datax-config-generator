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
    public static String MYSQL_DATABASE_IMPORT;
    public static String MYSQL_DATABASE_EXPORT;
    public static String MYSQL_URL_IMPORT;
    public static String MYSQL_URL_EXPORT;
    public static String MYSQL_TABLES_IMPORT;
    public static String MYSQL_TABLES_EXPORT;
    public static String IS_SEPERATED_TABLES;
    public static String HDFS_URI;
    public static String IMPORT_OUT_DIR;
    public static String EXPORT_OUT_DIR;
    public static String IMPORT_MIGRATION_TYPE = "import";
    public static String EXPORT_MIGRATION_TYPE = "export";

    static {
        Path path = Paths.get("configuration.properties");
        Properties configuration = new Properties();
        try {
            configuration.load(Files.newBufferedReader(path));
            MYSQL_USER = configuration.getProperty("mysql.username", "root");
            MYSQL_PASSWORD = configuration.getProperty("mysql.password", "000000");
            MYSQL_HOST = configuration.getProperty("mysql.host", "hadoop102");
            MYSQL_PORT = configuration.getProperty("mysql.port", "3306");
            MYSQL_DATABASE_IMPORT = configuration.getProperty("mysql.database.import", "gmall");
            MYSQL_DATABASE_EXPORT = configuration.getProperty("mysql.database.export", "gmall");
            MYSQL_URL_IMPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_IMPORT + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_URL_EXPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_EXPORT + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES_IMPORT = configuration.getProperty("mysql.tables.import", "");
            MYSQL_TABLES_EXPORT = configuration.getProperty("mysql.tables.export", "");
            IS_SEPERATED_TABLES = configuration.getProperty("is.seperated.tables", "0");
            HDFS_URI = configuration.getProperty("hdfs.uri", "hdfs://hadoop102:8020");
            IMPORT_OUT_DIR = configuration.getProperty("import_out_dir");
            EXPORT_OUT_DIR = configuration.getProperty("export_out_dir");
        } catch (IOException e) {
            MYSQL_USER = "root";
            MYSQL_PASSWORD = "000000";
            MYSQL_HOST = "hadoop102";
            MYSQL_PORT = "3306";
            MYSQL_DATABASE_IMPORT = "gmall";
            MYSQL_DATABASE_EXPORT = "gmall";
            MYSQL_URL_IMPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_IMPORT + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_URL_EXPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_EXPORT + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES_IMPORT = "";
            MYSQL_TABLES_EXPORT = "";
            IS_SEPERATED_TABLES = "0";
            HDFS_URI = "hdfs://hadoop102:8020";
            IMPORT_OUT_DIR = null;
            EXPORT_OUT_DIR = null;
        }
    }
}
