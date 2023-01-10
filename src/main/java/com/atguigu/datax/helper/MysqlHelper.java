package com.atguigu.datax.helper;

import com.atguigu.datax.beans.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlHelper {
    private final ConfigHelper config;
    private final List<Table> tables;


    public String getDatabase() {
        return config.MYSQL_DATABASE;
    }

    public List<Table> getTables() {
        return tables;
    }

    public MysqlHelper() {
        config = new ConfigHelper();
        tables = new ArrayList<>();

        // 获取表格
        Connection connection = getConnection();
        try {
            if (config.MYSQL_TABLES != null) {
                String[] tableNames = config.MYSQL_TABLES.split(",");
                PreparedStatement preparedStatement = connection.prepareStatement("select table_id from information_schema.INNODB_TABLES where name=?");
                for (String tableName : tableNames) {
                    preparedStatement.setString(1, config.MYSQL_DATABASE + "/" + tableName);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    resultSet.next();
                    long tableId = resultSet.getLong(1);
                    Table table = new Table(tableId, tableName);
                    tables.add(table);
                }
                preparedStatement.close();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement("select table_id, name from information_schema.INNODB_TABLES where name like ?");
                preparedStatement.setString(1, config.MYSQL_DATABASE + "%");
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    long tableId = resultSet.getLong(1);
                    String tableName = resultSet.getString(2).split("/")[1];
                    Table table = new Table(tableId, tableName);
                    tables.add(table);
                }
                preparedStatement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // 获取table的列
        for (Table table : tables) {
            try {
                PreparedStatement preparedStatement = connection.prepareStatement("select name, mtype from information_schema.INNODB_COLUMNS where TABLE_ID = ? order by POS");
                preparedStatement.setLong(1, table.id());
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    int type = resultSet.getInt(2);
                    table.addColumn(name, type);
                }
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    private Connection getConnection() {
        try {
            return DriverManager.getConnection(String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8", config.MYSQL_HOST, config.MYSQL_PORT, config.MYSQL_DATABASE), config.MYSQL_USER, config.MYSQL_PASSWORD);
        } catch (SQLException e) {
            System.out.println("建立MySQL连接发生错误，请检查host/port/username/password配置");
            throw new RuntimeException(e);
        }
    }


}
