package com.atguigu.datax.helper;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.configuration.Configuration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MysqlHelper {
    private final List<Table> tables;

    public List<Table> getTables() {
        return tables;
    }

    public MysqlHelper(String url, String database, String mysqlTables) {
        tables = new ArrayList<>();

        Db db = Db.use(DSFactory.create(
                Setting.create()
                        .set("url", url)
                        .set("user", Configuration.MYSQL_USER)
                        .set("pass", Configuration.MYSQL_PASSWORD)
                        .set("showSql", "false")
                        .set("showParams", "false")
                        .set("sqlLevel", "info")
        ).getDataSource());

        // 获取设置的表格，如未设置，查询数据库下面所有表格
        if (mysqlTables != null && !"".equals(mysqlTables)) {
            for (String mysqlTable : mysqlTables.split(",")) {
                tables.add(new Table(mysqlTable));
            }
        } else {
            try {
                db.findAll(Entity.create("information_schema.TABLES")
                                .set("TABLE_SCHEMA", database))
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
                                .set("TABLE_SCHEMA", database)
                                .set("TABLE_NAME", table.name())
                        ).stream()
                        .sorted(Comparator.comparingInt(o -> o.getInt("ORDINAL_POSITION")))
                        .forEach(entity -> table.addColumn(
                                entity.getStr("COLUMN_NAME"),
                                entity.getStr("DATA_TYPE")
                        ));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
