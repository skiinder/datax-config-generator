package com.atguigu.datax.helper;


import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.configuration.Configuration;

public class DataxJsonHelper {

    // 解析inputConfig和outputConfig模板
    private final JSONObject inputConfig = JSONUtil.parseObj("{\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"column\":[],\"connection\":[{\"jdbcUrl\":[],\"table\":[]}],\"password\":\"\",\"splitPk\":\"\",\"username\":\"\"}},\"writer\":{\"name\":\"hdfswriter\",\"parameter\":{\"column\":[],\"compress\":\"gzip\",\"defaultFS\":\"\",\"fieldDelimiter\":\"\\t\",\"fileName\":\"content\",\"fileType\":\"text\",\"path\":\"${targetdir}\",\"writeMode\":\"append\",\"nullFormat\":\"\"}}}],\"setting\":{\"speed\":{\"channel\":1}}}}");
    private final JSONObject outputConfig = JSONUtil.parseObj("{\"job\":{\"setting\":{\"speed\":{\"channel\":1}},\"content\":[{\"reader\":{\"name\":\"hdfsreader\",\"parameter\":{\"path\":\"${exportdir}\",\"defaultFS\":\"\",\"column\":[\"*\"],\"fileType\":\"text\",\"encoding\":\"UTF-8\",\"fieldDelimiter\":\"\\t\",\"nullFormat\":\"\\\\N\"}},\"writer\":{\"name\":\"mysqlwriter\",\"parameter\":{\"writeMode\":\"replace\",\"username\":\"\",\"password\":\"\",\"column\":[],\"connection\":[{\"jdbcUrl\":[],\"table\":[]}]}}}]}}");

    public DataxJsonHelper() {
        // 获取Reader和Writer配置
        JSONObject mysqlReaderPara = inputConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject hdfsWriterPara = inputConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);
        JSONObject hdfsReaderPara = outputConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject mysqlWriterPara = outputConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);

        // 设置DefaultFS
        hdfsReaderPara.set("defaultFS", Configuration.HDFS_URI);
        hdfsWriterPara.set("defaultFS", Configuration.HDFS_URI);

        // 设置MySQL Username
        mysqlReaderPara.set("username", Configuration.MYSQL_USER);
        mysqlWriterPara.set("username", Configuration.MYSQL_USER);

        // 设置MySQL Password
        mysqlReaderPara.set("password", Configuration.MYSQL_PASSWORD);
        mysqlWriterPara.set("password", Configuration.MYSQL_PASSWORD);

        // 设置JDBC URL
        mysqlReaderPara.putByPath("connection[0].jdbcUrl[0]", Configuration.MYSQL_URL);
        mysqlWriterPara.putByPath("connection[0].jdbcUrl[0]", Configuration.MYSQL_URL);

        // 写回Reader和Writer配置
        inputConfig.putByPath("job.content[0].reader.parameter", mysqlReaderPara);
        inputConfig.putByPath("job.content[0].writer.parameter", hdfsWriterPara);
        outputConfig.putByPath("job.content[0].reader.parameter", hdfsReaderPara);
        outputConfig.putByPath("job.content[0].writer.parameter", mysqlWriterPara);
    }

    public void setTableAndColumns(Table table) {
        // 设置表名
        inputConfig.putByPath("job.content[0].reader.parameter.connection[0].table[0]", table.name());
        outputConfig.putByPath("job.content[0].writer.parameter.connection[0].table[0]", table.name());

        // 设置列名
        inputConfig.putByPath("job.content[0].reader.parameter.column", table.getColumnNames());
        inputConfig.putByPath("job.content[0].writer.parameter.column", table.getColumns());
        outputConfig.putByPath("job.content[0].writer.parameter.column", table.getColumnNames());
    }

    public JSONObject getInputConfig() {
        return inputConfig;
    }

    public JSONObject getOutputConfig() {
        return outputConfig;
    }
}
