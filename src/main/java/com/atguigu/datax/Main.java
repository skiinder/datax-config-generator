package com.atguigu.datax;

import cn.hutool.json.JSONUtil;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.configuration.Configuration;
import com.atguigu.datax.helper.DataxJsonHelper;
import com.atguigu.datax.helper.MysqlHelper;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {

        // 生成 HDFS 入方向配置文件
        if (Configuration.IMPORT_OUT_DIR != null &&
                !Configuration.IMPORT_OUT_DIR.equals("")) {
            MysqlHelper mysqlHelper = new MysqlHelper(
                    Configuration.MYSQL_URL_IMPORT,
                    Configuration.MYSQL_DATABASE_IMPORT,
                    Configuration.MYSQL_TABLES_IMPORT);
            DataxJsonHelper dataxJsonHelper = new DataxJsonHelper();

            // 获取迁移操作类型
            String migrationType = Configuration.IMPORT_MIGRATION_TYPE;

            // 创建父文件夹
            Files.createDirectories(Paths.get(Configuration.IMPORT_OUT_DIR));
            List<Table> tables = mysqlHelper.getTables();

            // 判断传入的表是否为分表，根据判断结果采用不同的处理策略
            if (Configuration.IS_SEPERATED_TABLES.equals("1")) {
                for (int i = 0; i < tables.size(); i++) {
                    Table table = tables.get(i);
                    dataxJsonHelper.setTable(table, i, migrationType);
                }
                dataxJsonHelper.setColumns(tables.get(0), migrationType);

                // 输出最终Json配置
                FileWriter inputWriter = new FileWriter(Configuration.IMPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE_IMPORT + "." + tables.get(0).name() + ".json");
                JSONUtil.toJsonStr(dataxJsonHelper.getInputConfig(), inputWriter);
                inputWriter.close();
            } else {
                for (Table table : tables) {
                    // 设置表信息
                    dataxJsonHelper.setTableAndColumns(table, 0, migrationType);

                    // 输出最终Json配置
                    FileWriter inputWriter = new FileWriter(Configuration.IMPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE_IMPORT + "." + table.name() + ".json");
                    JSONUtil.toJsonStr(dataxJsonHelper.getInputConfig(), inputWriter);
                    inputWriter.close();
                }
            }
        }

        // 生成 HDFS 出方向配置文件
        if (Configuration.EXPORT_OUT_DIR != null &&
                !"".equals(Configuration.EXPORT_OUT_DIR)) {
            MysqlHelper mysqlHelper = new MysqlHelper(
                    Configuration.MYSQL_URL_EXPORT,
                    Configuration.MYSQL_DATABASE_EXPORT,
                    Configuration.MYSQL_TABLES_EXPORT);
            DataxJsonHelper dataxJsonHelper = new DataxJsonHelper();

            // 获取迁移操作类型
            String migrationType = Configuration.EXPORT_MIGRATION_TYPE;

            // 创建父文件夹
            Files.createDirectories(Paths.get(Configuration.EXPORT_OUT_DIR));
            List<Table> tables = mysqlHelper.getTables();

            if (Configuration.IS_SEPERATED_TABLES.equals("1")) {
                for (int i = 0; i < tables.size(); i++) {
                    Table table = tables.get(i);
                    dataxJsonHelper.setTable(table, i, migrationType);
                }
                dataxJsonHelper.setColumns(tables.get(0), migrationType);

                // 输出最终Json配置
                FileWriter outputWriter = new FileWriter(Configuration.EXPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE_EXPORT + "." + tables.get(0).name() + ".json");
                JSONUtil.toJsonStr(dataxJsonHelper.getOutputConfig(), outputWriter);
                outputWriter.close();
            }

            for (Table table : tables) {
                // 设置表信息
                dataxJsonHelper.setTableAndColumns(table, 0, migrationType);
                // 输出最终Json配置
                FileWriter outputWriter = new FileWriter(Configuration.EXPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE_EXPORT + "." + table.name() + ".json");
                JSONUtil.toJsonStr(dataxJsonHelper.getOutputConfig(), outputWriter);
                outputWriter.close();
            }
        }
    }
}
