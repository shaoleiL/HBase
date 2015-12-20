package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by shaolei on 2015/12/20 14:22.
 */
public class ScanApp {

    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() throws IOException {
        String tableName = "student";
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            Cell cell1 = result.getColumnLatestCell("f1".getBytes(), "name".getBytes());
            Cell cell2 = result.getColumnLatestCell("f1".getBytes(), "age".getBytes());
            System.out.println("rowkey : " + new String(row) +
                    "\t name : " + new String(CellUtil.cloneValue(cell1)) +
                    "\t age : "+ new String(CellUtil.cloneValue(cell2)));
        }
        hTable.close();

    }
}
