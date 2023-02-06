package com.atguigu.datax.helper;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.configuration.Configuration;

public class DataxJsonHelper {

    // 解析 inputConfig 和 outputConfig 模板

    // Hadoop 单点集群
    private final JSONObject inputConfig = JSONUtil.parseObj("{\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"column\":[],\"connection\":[{\"jdbcUrl\":[],\"table\":[]}],\"password\":\"\",\"splitPk\":\"\",\"username\":\"\"}},\"writer\":{\"name\":\"hdfswriter\",\"parameter\":{\"column\":[],\"compress\":\"gzip\",\"defaultFS\":\"\",\"fieldDelimiter\":\"\\t\",\"fileName\":\"content\",\"fileType\":\"text\",\"path\":\"${targetdir}\",\"writeMode\":\"truncate\",\"nullFormat\":\"\"}}}],\"setting\":{\"speed\":{\"channel\":1}}}}");
    private final JSONObject outputConfig = JSONUtil.parseObj("{\"job\":{\"setting\":{\"speed\":{\"channel\":1}},\"content\":[{\"reader\":{\"name\":\"hdfsreader\",\"parameter\":{\"path\":\"${exportdir}\",\"defaultFS\":\"\",\"column\":[\"*\"],\"fileType\":\"text\",\"encoding\":\"UTF-8\",\"fieldDelimiter\":\"\\t\",\"nullFormat\":\"\\\\N\"}},\"writer\":{\"name\":\"mysqlwriter\",\"parameter\":{\"writeMode\":\"replace\",\"username\":\"\",\"password\":\"\",\"column\":[],\"connection\":[{\"jdbcUrl\":\"\",\"table\":[]}]}}}]}}");

    // Hadoop HA 集群
//    private final JSONObject inputConfig = JSONUtil.parseObj("{\"job\": {\"content\": [{\"reader\": {\"name\": \"mysqlreader\",\"parameter\": {\"column\": [],\"connection\": [{\"jdbcUrl\": [],\"table\": []}],\"password\": \"\",\"splitPk\": \"\",\"username\": \"\"}},\"writer\": {\"name\": \"hdfswriter\",\"parameter\": {\"column\": [],\"compress\": \"gzip\",\"defaultFS\": \"hdfs://mycluster\",\"dfs.nameservices\": \"mycluster\",\"dfs.ha.namenodes.mycluster\": \"namenode1,namenode2\",\"dfs.namenode.rpc-address.aliDfs.namenode1\": \"hdfs://com.tstzyls-hadoop101:8020\",\"dfs.namenode.rpc-address.aliDfs.namenode2\": \"hdfs://com.tstzyls-hadoop102:8020\",\"dfs.client.failover.proxy.provider.mycluster\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"fieldDelimiter\": \"\\t\",\"fileName\": \"content\",\"fileType\": \"text\",\"path\": \"${targetdir}\",\"writeMode\": \"truncate\",\"nullFormat\": \"\"}}}],\"setting\": {\"speed\": {\"channel\": 1}}}}");
//    private final JSONObject outputConfig = JSONUtil.parseObj("{\"job\": {\"setting\": {\"speed\": {\"channel\": 1}},\"content\": [{\"reader\": {\"name\": \"hdfsreader\",\"parameter\": {\"path\": \"${exportdir}\",\"defaultFS\": \"\",\"dfs.nameservices\": \"mycluster\",\"dfs.ha.namenodes.mycluster\": \"namenode1,namenode2\",\"dfs.namenode.rpc-address.aliDfs.namenode1\": \"hdfs://com.tstzyls-hadoop101:8020\",\"dfs.namenode.rpc-address.aliDfs.namenode2\": \"hdfs://com.tstzyls-hadoop102:8020\",\"dfs.client.failover.proxy.provider.mycluster\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"column\": [\"*\"],\"fileType\": \"text\",\"encoding\": \"UTF-8\",\"fieldDelimiter\": \"\\t\",\"nullFormat\": \"\\\\N\"}},\"writer\": {\"name\": \"mysqlwriter\",\"parameter\": {\"writeMode\": \"replace\",\"username\": \"\",\"password\": \"\",\"column\": [],\"connection\": [{\"jdbcUrl\": [],\"table\": []}]}}}]}}");

    public DataxJsonHelper() {
        // 获取 Reader 和 Writer 配置
        JSONObject mysqlReaderPara = inputConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject hdfsWriterPara = inputConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);
        JSONObject hdfsReaderPara = outputConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject mysqlWriterPara = outputConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);

        // 设置 DefaultFS
        hdfsReaderPara.set("defaultFS", Configuration.HDFS_URI);
        hdfsWriterPara.set("defaultFS", Configuration.HDFS_URI);

        // 设置 MySQL Username
        mysqlReaderPara.set("username", Configuration.MYSQL_USER);
        mysqlWriterPara.set("username", Configuration.MYSQL_USER);

        // 设置 MySQL Password
        mysqlReaderPara.set("password", Configuration.MYSQL_PASSWORD);
        mysqlWriterPara.set("password", Configuration.MYSQL_PASSWORD);

        // 设置 JDBC URL
        mysqlReaderPara.putByPath("connection[0].jdbcUrl[0]", Configuration.MYSQL_URL_IMPORT);
        mysqlWriterPara.putByPath("connection[0].jdbcUrl", Configuration.MYSQL_URL_EXPORT);

        // 写回Reader和Writer配置
        inputConfig.putByPath("job.content[0].reader.parameter", mysqlReaderPara);
        inputConfig.putByPath("job.content[0].writer.parameter", hdfsWriterPara);
        outputConfig.putByPath("job.content[0].reader.parameter", hdfsReaderPara);
        outputConfig.putByPath("job.content[0].writer.parameter", mysqlWriterPara);
    }

    public void setTableAndColumns(Table table, int index, String migrationType) {
        // 设置表名
        setTable(table, index, migrationType);
        // 设置列名及路径
        setColumns(table, migrationType);
    }

    public void setColumns(Table table, String migrationType) {
        if (migrationType.equals("import")) {
            // 设置 hdfswriter 文件名
            inputConfig.putByPath("job.content[0].writer.parameter.fileName", table.name());
            // 设置列名
            inputConfig.putByPath("job.content[0].reader.parameter.column", table.getColumnNames());
            inputConfig.putByPath("job.content[0].writer.parameter.column", table.getColumnNamesAndTypes());
        } else {
            // 设置列名
            outputConfig.putByPath("job.content[0].writer.parameter.column", table.getColumnNames());
        }
    }

    public void setTable(Table table, int index, String migrationType) {
        if (migrationType.equals("import")) {
            // 设置表名
            inputConfig.putByPath("job.content[0].reader.parameter.connection[0].table[" + index + "]", table.name());
        } else {
            outputConfig.putByPath("job.content[0].writer.parameter.connection[0].table[" + index + "]", table.name());
        }
    }

    public JSONObject getInputConfig() {
        return inputConfig;

    }

    public JSONObject getOutputConfig() {
        return outputConfig;
    }
}
