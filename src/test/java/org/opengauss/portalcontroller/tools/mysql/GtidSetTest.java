package org.opengauss.portalcontroller.tools.mysql;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

public class GtidSetTest {

    @Test
    @DisplayName("单个事务  3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
    public void changeGtidSetCase1() {
        String gtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:23";
        String uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        String expected = "3e11fa47-71ca-11e1-9e33-c80aa9429562:23";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }

    @Test
    @DisplayName("连续事务 3e8d4d5a-74d9-4d81-8f89-8c898c898c89:1-2")
    public void changeGtidSetCase2() {
        String gtidSet = "3e8d4d5a-74d9-4d81-8f89-8c898c898c89:1-2";
        String uuid = "3e8d4d5a-74d9-4d81-8f89-8c898c898c89";
        String expected = "3e8d4d5a-74d9-4d81-8f89-8c898c898c89:1-1";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }

    @Test
    @DisplayName("连续事务 b558713b-3203-11ef-a751-fa163e5bb398:1-33")
    public void changeGtidSetCase3() {
        String gtidSet = "b558713b-3203-11ef-a751-fa163e5bb398:1-33";
        String uuid = "b558713b-3203-11ef-a751-fa163e5bb398";
        String expected = "b558713b-3203-11ef-a751-fa163e5bb398:1-32";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }

    @Test
    @DisplayName("多个连续事务 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3:11:47-49")
    public void changeGtidSetCase4() {
        String gtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3:11:47-49";
        String uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        String expected = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3:11:47-49";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }

    @Test
    @DisplayName("多个连续事务 2174b383-5441-11e8-b90a-c80aa9429562:1-3,24da167-0c0c-11e8-8442-00059a3c7b00:1-19")
    public void changeGtidSetCase5() {
        String gtidSet = "2174b383-5441-11e8-b90a-c80aa9429562:1-3,24da167-0c0c-11e8-8442-00059a3c7b00:1-19";
        String uuid = "24da167-0c0c-11e8-8442-00059a3c7b00";
        String expected = "2174b383-5441-11e8-b90a-c80aa9429562:1-3,24da167-0c0c-11e8-8442-00059a3c7b00:1-18";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }


    @Test
    @DisplayName("多个连续事务 2174b383-5441-11e8-b90a-c80aa9429562:1-3,24da167-0c0c-11e8-8442-00059a3c7b00:1-19")
    public void changeGtidSetCase6() {
        String gtidSet = "13868a28-3220-11ef-9d3b-fa163e5bb398:1-27146," +System.lineSeparator()+
                "4fdcd7b8-321b-11ef-8226-fa163e5bb398:1-3," +System.lineSeparator()+
                "57f3dbef-31f9-11ef-b2fc-fa163e5bb398:1-3," +System.lineSeparator()+
                "67f3400b-344b-11ef-adb8-fa163e5bb398:1," +System.lineSeparator()+
                "788de19d-3203-11ef-8dcb-fa163e5bb398:1," +System.lineSeparator()+
                "b558713b-3203-11ef-a751-fa163e5bb398:1-33";
        String uuid = "b558713b-3203-11ef-a751-fa163e5bb398";
        String expected = "13868a28-3220-11ef-9d3b-fa163e5bb398:1-27146,4fdcd7b8-321b-11ef-8226-fa163e5bb398:1-3," +
                "57f3dbef-31f9-11ef-b2fc-fa163e5bb398:1-3,67f3400b-344b-11ef-adb8-fa163e5bb398:1,788de19d-3203-11ef-8dcb-fa163e5bb398:1," +
                "b558713b-3203-11ef-a751-fa163e5bb398:1-32";
        String result = IncrementalMigrationTool.changeGtidSet(gtidSet, uuid);
        Assert.assertEquals(expected, result);
    }
}
