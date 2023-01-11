package com.atguigu.datax.beans;

import java.util.*;
import java.util.stream.Collectors;

public class Table {
    private final String tableName;
    private final List<Column> columns;

    public Table(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();

    }

    public String name() {
        return tableName;
    }

    public void addColumn(String name, String type) {
        columns.add(new Column(name, type));
    }

    public List<String> getColumnNames() {
        return columns.stream().map(Column::name).collect(Collectors.toList());
    }

    public List<Map<String, String>> getColumns() {
        List<Map<String, String>> result = new ArrayList<>();
        columns.forEach(column -> {
            Map<String, String> temp = new HashMap<>();
            temp.put("name", column.name());
            temp.put("type", column.hiveType());
            result.add(temp);
        });
        return result;
    }
}
