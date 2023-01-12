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

public class Main {
    public static void main(String[] args) throws IOException {
        MysqlHelper mysqlHelper = new MysqlHelper();
        DataxJsonHelper dataxJsonHelper = new DataxJsonHelper();

        // 创建父文件夹
        if (Configuration.IMPORT_OUT_DIR != null) {
            Files.createDirectories(Paths.get(Configuration.IMPORT_OUT_DIR));
            for (Table table : mysqlHelper.getTables()) {
                // 设置表信息
                dataxJsonHelper.setTableAndColumns(table);
                // 输出最终Json配置
                FileWriter inputWriter = new FileWriter(Configuration.IMPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE + "." + table.name() + ".json");
                JSONUtil.toJsonStr(dataxJsonHelper.getInputConfig(), inputWriter);
                inputWriter.close();
            }
        }
        if (Configuration.EXPORT_OUT_DIR != null) {
            Files.createDirectories(Paths.get(Configuration.EXPORT_OUT_DIR));
            for (Table table : mysqlHelper.getTables()) {
                // 设置表信息
                dataxJsonHelper.setTableAndColumns(table);
                // 输出最终Json配置
                FileWriter outputWriter = new FileWriter(Configuration.EXPORT_OUT_DIR + "/" + Configuration.MYSQL_DATABASE + "." + table.name() + ".json");
                JSONUtil.toJsonStr(dataxJsonHelper.getOutputConfig(), outputWriter);
                outputWriter.close();
            }
        }

    }
}
