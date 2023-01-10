package com.atguigu.datax.beans;

import java.util.HashMap;
import java.util.Map;

public class Table {
    private final String tableName;
    private final Map<String, Integer> columns;

    private final long tableId;

    private static final Map<Integer, String> typeMap = new HashMap<>();

    static {
        typeMap.put(1, "string");
        typeMap.put(2, "string");
        typeMap.put(3, "string");
        typeMap.put(4, "string");
        typeMap.put(5, "string");
        typeMap.put(6, "bigint");
        typeMap.put(7, "string");
        typeMap.put(8, "string");
        typeMap.put(9, "float");
        typeMap.put(10, "double");
        typeMap.put(11, "string");
        typeMap.put(12, "string");
        typeMap.put(13, "string");
        typeMap.put(14, "string");
    }

    public Table(long tableId, String tableName) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.columns = new HashMap<>();

    }

    public long id() {
        return tableId;
    }

    public String name() {
        return tableName;
    }

    public void addColumn(String name, int type) {
        columns.put(name, type);
    }

    public Map<String, Integer> getColumns() {
        return columns;
    }

    public Map<String, String> getColumnsAsHiveType() {
        Map<String, String> result = new HashMap<>();
        columns.forEach((x, y) -> result.put(x, typeMap.get(y)));
        return result;
    }
}
