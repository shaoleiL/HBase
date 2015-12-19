package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by shaolei on 2015/12/19 15:34.
 */
public class CURDApp {
    private Configuration conf = null;
    @Before
    public void init(){
        conf = HBaseConfiguration.create();
    }

    /**
     * 根据rowkey查询数据
     * @throws IOException
     */
    @Test
    public void getData() throws IOException {
        HTable table = new HTable(conf,"person");
        Get get = new Get(Bytes.toBytes("rk1001"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
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

        for(Cell cell : cells){
            System.out.print("rowkey:"+new String(CellUtil.cloneRow(cell))+"\t");
            System.out.print("Timetamp:" + cell.getTimestamp() + "\t");
            System.out.print("column Family:" + new String(CellUtil.cloneFamily(cell)) + "\t");
            System.out.print("row Name:" + new String(CellUtil.cloneQualifier(cell)) + "\t");
            System.out.print("value:" + new String(CellUtil.cloneValue(cell)) + "\t");
            System.out.println();
        }
        table.close();
    }

    /**
     * 查询表中的所有数据
     * @throws IOException
     */
    @Test
    public void getAllData() throws IOException {
        HTable table = new HTable(conf,"test");
        Scan scan=new Scan();
        scan.setMaxVersions(5);
        ResultScanner results=table.getScanner(scan);
        for(Result result:results) {
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
    }
}
