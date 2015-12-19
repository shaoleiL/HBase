package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * HBase的JavaAPI操作
 * Created by shaolei on 2015/12/19 15:34.
 */
public class CURDApp {
    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
    }

    /**
     * 根据rowkey查询数据
     *
     * @throws IOException
     */
    @Test
    public void getData() throws IOException {
        String rawkey = "rk1001";
        HTable table = new HTable(conf, "person");
        Get get = new Get(Bytes.toBytes(rawkey));
        get.setMaxVersions(5);

        //以下方式会产生乱码
//        for (Cell cell : cells) {
//            byte[] familyArray = cell.getFamilyArray();
//            String family = new String(familyArray);
//            System.out.println(family + "");
//            byte[] qualifierArray = cell.getQualifierArray();
//            String qualifier = new String(qualifierArray);
//            System.out.println(qualifier);
//        }
        //以下方式方法已过时
//        for(KeyValue kv : result.list()){
//            String family = new String(kv.getFamily());
//            System.out.println(family);
//            String qualifier = new String(kv.getQualifier());
//            System.out.println(qualifier);
//            System.out.println(new String(kv.getValue()));
//        }
        Result result = table.get(get);
        if (result.rawCells().length == 0){
            System.out.println("不存在" + rawkey + "的行");
        }else {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.print("rowkey:" + new String(CellUtil.cloneRow(cell)) + "\t");
                System.out.print("Timetamp:" + cell.getTimestamp() + "\t");
                System.out.print("column Family:" + new String(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("row Name:" + new String(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.print("value:" + new String(CellUtil.cloneValue(cell)) + "\t");
                System.out.println();
            }
        }
        table.close();
    }

    /**
     * 查询表中的所有数据
     *
     * @throws IOException
     */
    @Test
    public void getAllData() throws IOException {
        HTable table = new HTable(conf, "student");
        Scan scan = new Scan();
        scan.setMaxVersions(5);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.print("rowKey:" + new String(CellUtil.cloneRow(cell)) + "\t");
                System.out.print("Timetamp:" + cell.getTimestamp() + "\t");
                System.out.print("column Family:" + new String(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("row Name:" + new String(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.print("value:" + new String(CellUtil.cloneValue(cell)) + "\t");
                System.out.println();
            }
        }
        table.close();
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable() throws IOException {
        String htableName = "student";
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(htableName);
        admin.deleteTable(htableName);
        admin.close();
        System.out.println("删除成功");
    }

    /**
     * 创建一张表
     */
    @Test
    public void createHTable() throws IOException {
        String tableName = "student";
        String family1 = "f1";
        String family2 = "f2";
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + "表已存在");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            hTableDescriptor.addFamily(new HColumnDescriptor(family1));
            hTableDescriptor.addFamily(new HColumnDescriptor(family2));
            admin.createTable(hTableDescriptor);
            System.out.println(tableName + "表创建成功");
        }
        admin.close();
    }

    /**
     * 添加数据
     * @throws IOException
     */
    @Test
    public void insertData() throws IOException {

        String tableName = "student"; //表名:student
        String rowkey1001 = "rk1001"; //行健:rk1001
        String family1 = "f1"; //列簇:f1  学生信息
        String rk1f1name = "name";
        String rk1f1nameValue = "张三";
        String rk1f1age = "age";
        String rk1f1ageValue = "22";

        String family2 = "f2"; //列簇:f2  各科成绩
        String rk1f2chinese = "chinese";
        String rk1f2chineseScore = "90";
        String rk1f2math = "math";
        String rk1f2mathScore = "95";
        String rk1f2english = "english";
        String rk1f2englishScore = "89";

        String rowkey1002 = "rk1002";
        String rk2f1name = "name";
        String rk2f1nameValue = "李四";
        String rk2f1age = "age";
        String rk2f1ageValue = "25";
        HTable hTable = new HTable(conf, tableName);
        Put put1 = new Put(rowkey1001.getBytes()); //行健:rk1001
        put1.add(family1.getBytes(), rk1f1name.getBytes(), rk1f1nameValue.getBytes());
        put1.add(family1.getBytes(), rk1f1age.getBytes(), rk1f1ageValue.getBytes());
        put1.add(family2.getBytes(), rk1f2chinese.getBytes(), rk1f2chineseScore.getBytes());
        put1.add(family2.getBytes(), rk1f2math.getBytes(), rk1f2mathScore.getBytes());
        put1.add(family2.getBytes(), rk1f2english.getBytes(), rk1f2englishScore.getBytes());
        Put put2 = new Put(rowkey1002.getBytes()); //行健:rk1002
        put2.add(family1.getBytes(), rk2f1name.getBytes(), rk2f1nameValue.getBytes());
        put2.add(family1.getBytes(), rk2f1age.getBytes(), rk2f1ageValue.getBytes());

        hTable.put(put1);
        hTable.put(put2);
        hTable.close();
        System.out.println("添加记录" + rowkey1001 + "成功");
        System.out.println("添加记录" + rowkey1002 + "成功");
    }

    /**
     * 删除行记录
     */
    @Test
    public void deleteRecord() {
        String tableName = "student"; //表名:student
        String rowkey1001 = "rk1001"; //行健:rk1001
        String rowkey1002 = "rk1002";
        HTable hTable = null;
        try {
            hTable = new HTable(conf, tableName);
            Delete delete1 = new Delete(rowkey1001.getBytes());
            Delete delete2 = new Delete(rowkey1002.getBytes());
            hTable.delete(delete1);
            hTable.delete(delete2);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                assert hTable != null;
                hTable.close();
                System.out.println("删除记录 " + rowkey1001 + " " + rowkey1002 + "成功");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



    }
}
