package com.atguigu.datax.beans;

import java.util.HashMap;
import java.util.Map;

public class Column {
    private final String name;
    private final String type;
    private final String hiveType;
    private static final Map<String, String> typeMap = new HashMap<>();

    static {
        typeMap.put("bigint", "bigint");
        typeMap.put("int", "bigint");
        typeMap.put("smallint", "bigint");
        typeMap.put("tinyint", "bigint");
        typeMap.put("double", "double");
        typeMap.put("float", "float");
    }

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
        this.hiveType = typeMap.getOrDefault(type, "string");
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String hiveType() {
        return hiveType;
    }
}
