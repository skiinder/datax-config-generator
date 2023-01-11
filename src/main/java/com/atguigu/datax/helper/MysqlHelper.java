package com.atguigu.datax.helper;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.configuration.Configuration;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlHelper {
    private final List<Table> tables;

    public List<Table> getTables() {
        return tables;
    }

    public MysqlHelper() {
        tables = new ArrayList<>();

        Db db = Db.use(DSFactory.create(
                Setting.create()
                        .set("url", Configuration.MYSQL_URL)
                        .set("user", Configuration.MYSQL_USER)
                        .set("pass", Configuration.MYSQL_PASSWORD)
                        .set("showSql", "false")
                        .set("showParams", "false")
                        .set("sqlLevel", "info")
        ).getDataSource());

        // 获取设置的表格，如未设置，查询数据库下面所有表格
        if (Configuration.MYSQL_TABLES.length != 0) {
            for (String mysqlTable : Configuration.MYSQL_TABLES) {
                tables.add(new Table(mysqlTable));
            }
        } else {
            try {
                db.findAll(Entity.create("information_schema.TABLES")
                                .set("TABLE_SCHEMA", Configuration.MYSQL_DATABASE))
                        .forEach(entity ->
                                tables.add(new Table(entity.getStr("TABLE_NAME"))));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        // 获取所有表格的列
        for (Table table : tables) {
            try {
                db.findAll(Entity.create("information_schema.COLUMNS")
                        .set("TABLE_SCHEMA", Configuration.MYSQL_DATABASE)
                        .set("TABLE_NAME", table.name())
                ).forEach(entity -> table.addColumn(
                        entity.getStr("COLUMN_NAME"),
                        entity.getStr("DATA_TYPE")
                ));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
