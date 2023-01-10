package com.atguigu.datax;

import cn.hutool.json.JSONUtil;
import com.atguigu.datax.beans.Table;
import com.atguigu.datax.helper.ConfigHelper;
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
        ConfigHelper config = new ConfigHelper();

        for (Table table : mysqlHelper.getTables()) {
            // 设置表信息
            dataxJsonHelper.setTableAndColumns(table);
            // 输出最终Json配置
            Files.createDirectories(Paths.get(config.OUT_DIR + "/input"));
            Files.createDirectories(Paths.get(config.OUT_DIR + "/output"));
            FileWriter inputWriter = new FileWriter(config.OUT_DIR + "/input/" + table.name() + ".json");
            JSONUtil.toJsonStr(dataxJsonHelper.getInputConfig(), inputWriter);
            FileWriter outputWriter = new FileWriter(config.OUT_DIR + "/output/" + table.name() + ".json");
            JSONUtil.toJsonStr(dataxJsonHelper.getOutputConfig(), outputWriter);
        }

    }
}
