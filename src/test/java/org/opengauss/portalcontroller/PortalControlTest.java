/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
package org.opengauss.portalcontroller;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class PortalControlTest {
    @Test
    public void initPlanListTest(){
        PortalControl.initPlanList();
        List<String> plan1 = PortalControl.planList.get("plan1");
        assert plan1.contains("start mysql full migration");
        assert plan1.contains("start mysql full migration datacheck");
        List<String> plan2 = PortalControl.planList.get("plan2");
        assert plan2.contains("start mysql full migration");
        assert plan2.contains("start mysql incremental migration");
        List<String> plan3 = PortalControl.planList.get("plan3");
        assert plan3.contains("start mysql full migration");
        assert plan3.contains("start mysql full migration datacheck");
        assert plan3.contains("start mysql incremental migration");
    }

    @Test
    public void initHashTableTest() throws IOException {
        File file1 = new File("src/test/resources/toolspath.properties");
        file1.createNewFile();
        PortalControl.toolsConfigPath = file1.getAbsolutePath();
        FileWriter fw = new FileWriter(file1);
        fw.write("confluent.path=/data/lt/test/debezium/confluent/" + System.lineSeparator());
        fw.write("debezium.path=/data1/lt/test/debezium/" + System.lineSeparator());
        fw.flush();
        fw.close();
        File file2 = new File("src/test/resources/migrationConfig.properties");
        file2.createNewFile();
        PortalControl.migrationConfigPath = file2.getAbsolutePath();
        fw = new FileWriter(file2);
        fw.write("port=2345" + System.lineSeparator());
        fw.write("test=testHashtable" + System.lineSeparator());
        fw.flush();
        fw.close();
        PortalControl.initHashTable();
        assert PortalControl.toolsConfigParametersTable.containsKey("confluent.path");
        assert PortalControl.toolsConfigParametersTable.get("debezium.path").equals("/data1/lt/test/debezium/");
        assert PortalControl.toolsMigrationParametersTable.containsKey("port");
        assert PortalControl.toolsMigrationParametersTable.get("test").equals("testHashtable");
        PortalControl.toolsConfigParametersTable.clear();
        PortalControl.toolsMigrationParametersTable.clear();
        file1.delete();
        file2.delete();
    }

    @Test
    public void checkPathTest(){
        File file1 =new File("test");
        file1.mkdir();
        PortalControl.portalControlPath = file1.getAbsolutePath();
        File file2 =new File("toolsTest");
        file2.mkdir();
        PortalControl.toolsConfigPath = file2.getAbsolutePath();
        File file3 =new File("migrationTest");
        file3.mkdir();
        PortalControl.migrationConfigPath = file3.getAbsolutePath();
        assert PortalControl.checkPath();
        file1.delete();
        file2.delete();
        file3.delete();
    }

}
