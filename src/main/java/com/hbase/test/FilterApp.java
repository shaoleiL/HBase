package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 过滤器的使用
 * Created by shaolei on 2015/12/20 15:32.
 */
public class FilterApp {

    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
    }

    /**
     * 行健过滤器
     * @throws IOException
     */
    @Test
    public void rowFilterTest() throws IOException {
        String tableName = "student";
        String rowkey = "rk1002";
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(rowkey.getBytes()));
        scan.setFilter(filter);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell cell1 = result.getColumnLatestCell("f1".getBytes(), "name".getBytes());
            System.out.println("name : " + new String(CellUtil.cloneValue(cell1)));
        }
        hTable.close();
    }

    /**
     * 过滤器列表
     */
    @Test
    public void filterListTest() throws IOException, DeserializationException {
        String tableName = "student";
        String rowkey1 = "rk1003";
        String rowkey2 = "rk1006";
        String f1 = "f1";
        String f2 = "f2";
        String f1name = "name";
        String f2chinese = "chinese";
        String f1nameValue1 = "马六";
        String f1nameValue2 = "田七";
        String chinese1 = "90";
        String chinese2 = "99";
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //rowFilter
//        Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(rowkey2.getBytes()));
//        Filter filter2 = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(rowkey1.getBytes()));
        //SingleColumnValueFilter
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(f2),Bytes.toBytes(f2chinese), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(chinese1));
        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(f2),Bytes.toBytes(f2chinese), CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(chinese2));
//        Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,ByteArrayComparable.parseFrom(chinese1.getBytes()));
//        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,ByteArrayComparable.parseFrom(chinese2.getBytes()));
        list.addFilter(filter1);
        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell cell1 = result.getColumnLatestCell(f1.getBytes(), f1name.getBytes());
            Cell cell2 = result.getColumnLatestCell(f2.getBytes(), f2chinese.getBytes());
            System.out.println(new String(result.getRow()) + " name: " + new String(CellUtil.cloneValue(cell1)) + "\t" +
                "chinese:" + new String(CellUtil.cloneValue(cell2))
            );

        }
        hTable.close();
    }
}
