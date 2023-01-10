package com.atguigu.datax.helper;


import cn.hutool.core.bean.BeanPath;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.atguigu.datax.beans.Table;

import java.util.*;

public class DataxJsonHelper {

    // 解析inputConfig和outputConfig模板
    private final JSONObject inputConfig = JSONUtil.parseObj("{\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"column\":[],\"connection\":[{\"jdbcUrl\":\"\",\"table\":\"\"}],\"password\":\"\",\"splitPk\":\"\",\"username\":\"\"}},\"writer\":{\"name\":\"hdfswriter\",\"parameter\":{\"column\":[],\"compress\":\"gzip\",\"defaultFS\":\"\",\"fieldDelimiter\":\"\\t\",\"fileName\":\"content\",\"fileType\":\"text\",\"path\":\"${targetdir}\",\"writeMode\":\"append\",\"nullFormat\":\"\"}}}],\"setting\":{\"speed\":{\"channel\":1}}}}");
    private final JSONObject outputConfig = JSONUtil.parseObj("{\"job\":{\"setting\":{\"speed\":{\"channel\":1}},\"content\":[{\"reader\":{\"name\":\"hdfsreader\",\"parameter\":{\"path\":\"${exportdir}\",\"defaultFS\":\"\",\"column\":[\"*\"],\"fileType\":\"text\",\"encoding\":\"UTF-8\",\"fieldDelimiter\":\"\\t\",\"nullFormat\":\"\\\\N\"}},\"writer\":{\"name\":\"mysqlwriter\",\"parameter\":{\"writeMode\":\"replace\",\"username\":\"\",\"password\":\"\",\"column\":[],\"connection\":[{\"jdbcUrl\":\"\",\"table\":\"\"}]}}}]}}");

    public DataxJsonHelper() {
        BeanPath path;
        // 获取Reader和Writer配置
        path = new BeanPath("job.content[0].reader");
        JSONObject mysqlReader = JSONUtil.parseObj(path.get(inputConfig).toString());
        JSONObject hdfsReader = JSONUtil.parseObj(path.get(outputConfig).toString());
        path = new BeanPath("job.content[0].writer");
        JSONObject hdfsWriter = JSONUtil.parseObj(path.get(inputConfig).toString());
        JSONObject mysqlWriter = JSONUtil.parseObj(path.get(outputConfig).toString());

        // 设置DefaultFS
        path = new BeanPath("parameter.defaultFS");
        ConfigHelper config = new ConfigHelper();
        path.set(hdfsReader, config.HDFS_URI);
        path.set(hdfsWriter, config.HDFS_URI);

        // 设置MySQL Username
        path = new BeanPath("parameter.username");
        path.set(mysqlReader, config.MYSQL_USER);
        path.set(mysqlWriter, config.MYSQL_USER);

        // 设置MySQL Password
        path = new BeanPath("parameter.password");
        path.set(mysqlReader, config.MYSQL_PASSWORD);
        path.set(mysqlWriter, config.MYSQL_PASSWORD);

        // 设置JDBC URL
        path = new BeanPath("parameter.connection[0].jdbcUrl");
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8", config.MYSQL_HOST, config.MYSQL_PORT, config.MYSQL_DATABASE);
        path.set(mysqlReader, jdbcUrl);
        path.set(mysqlWriter, jdbcUrl);

        // 写回Reader和Writer配置
        path = new BeanPath("job.content[0].reader");
        path.set(inputConfig, mysqlReader);
        path.set(outputConfig, hdfsReader);
        path = new BeanPath("job.content[0].writer");
        path.set(inputConfig, hdfsWriter);
        path.set(outputConfig, mysqlWriter);
    }

    public void setTableAndColumns(Table table) {
        // 设置表名
        BeanPath path = new BeanPath("job.content[0].reader.parameter.connection[0].table");
        path.set(inputConfig, table.name());
        path = new BeanPath("job.content[0].writer.parameter.connection[0].table");
        path.set(outputConfig, table.name());

        // 设置列名
        path = new BeanPath("job.content[0].reader.parameter.column");
        path.set(inputConfig, table.getColumns().keySet());
        path = new BeanPath("job.content[0].writer.parameter.column");
        List<Map<String, String>> mapList = new ArrayList<>();
        table.getColumnsAsHiveType().forEach((x, y) -> {
            HashMap<String, String> kv = new HashMap<>();
            kv.put("name", x);
            kv.put("type", y);
            mapList.add(kv);
        });
        path.set(inputConfig, mapList);
        path.set(outputConfig, table.getColumns().keySet());

    }

    public JSONObject getInputConfig() {
        return inputConfig;
    }

    public JSONObject getOutputConfig() {
        return outputConfig;
    }
}
