package com.my.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.filter.RegexFilter;

import java.io.IOException;
import java.util.List;

/**
 * scan api操作hbase
 */
public class ScanTest {
    public static Configuration conf = HBaseConfiguration.create();
    public static Connection connection;
    public static Admin admin;
    public static Table table;

    static {
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        //可以实时加载配置文件
        //config.addResource(new Path("src/main/resources/hbase-site.xml"));
        //config.addResource(new Path("src/main/resources/core-site.xml"));
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            table = connection.getTable(TableName.valueOf(HbaseContent.TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("初始化hbase connection失败！");
        }
    }

    public static void main(String[] args) throws IOException {
//        basicScanTest();
//        rowFilterScanTest();
//        columnValueFilterScanTest();
//        pageFilterScanTest();
        filterListScanTest();
    }

    public static void basicScanTest() throws IOException {
        Scan scan = new Scan();
        //scan.addFamily()  //设置要扫描的family
        //scan.addColumn()//设置要扫描的column
        scan.setCaching(20);//设置每次rpc读取的记录数
        scan.setBatch(2); //每条记录数的column数量(如果该数值大于 column families * column per family， 则以后者数量为准)
//scan指定范围内的row
//        scan.setStartRow()//设置起始row
//        scan.setStopRow()//设置终止row
        ResultScanner results = table.getScanner(scan);
        //打印查询结果
        printResult(results);
    }

    /**
     * scan rowkey过滤查询
     *
     * @throws IOException
     */
    public static void rowFilterScanTest() throws IOException {
        Scan scan = new Scan();
        //查询row大于row_10的数据
//        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("new_row_10"));
//        RowFilter filter = new RowFilter(RowFilter.CompareOp.GREATER, binaryComparator);

        //查询出rowkey以5结尾的数据
        RegexStringComparator regexStringComparator = new RegexStringComparator(".5");
        //RowFilter filter = new RowFilter(RowFilter.CompareOp.EQUAL, regexStringComparator);

        //查询出rowkey不包含1的数据
        SubstringComparator comparator = new SubstringComparator("1");
        RowFilter filter = new RowFilter(RowFilter.CompareOp.NOT_EQUAL, comparator);

        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        printResult(results);
    }

    /**
     * scan column过滤查询
     *
     * @throws IOException
     */
    public static void columnFilterScanTest() throws IOException {
        Scan scan = new Scan();
        //查询column大于new_num的数据
//        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("new_num"));
//        FamilyFilter filter = new FamilyFilter(FamilyFilter.CompareOp.GREATER, binaryComparator);

        //查询出column不以f结尾的数据
//        RegexStringComparator regexStringComparator = new RegexStringComparator(".f");
//        FamilyFilter filter = new FamilyFilter(FamilyFilter.CompareOp.NOT_EQUAL, regexStringComparator);

        //查询出column不包含1的数据
//        SubstringComparator comparator = new SubstringComparator("new");
//        FamilyFilter filter = new FamilyFilter(FamilyFilter.CompareOp.NOT_EQUAL, comparator);

        //查询qualifier等于in的数据
//        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("in"));
//        QualifierFilter filter = new QualifierFilter(QualifierFilter.CompareOp.EQUAL, binaryComparator);
        //查询指定cloumn范围的数据
        String startColumn = "aaaaaa";
        String endColumn = "zzzzzz";
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(startColumn), true, Bytes.toBytes(endColumn), true);

        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        printResult(results);
    }

    public static void printResult(ResultScanner results) throws IOException {
        Result result = results.next();
        while (result != null) {
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print(" ");
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print(":");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.print(" ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }

            result = results.next();
        }

    }

    /**
     * scan column value过滤查询
     *
     * @throws IOException
     */
    public static void columnValueFilterScanTest() throws IOException {
        Scan scan = new Scan();

        //查找defualt_f中value以5为结尾的数据
        scan.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null); //一般加上column 过滤，否则全表扫描，性能低
        RegexStringComparator regexStringComparator = new RegexStringComparator(".5$");
        ValueFilter filter = new ValueFilter(ValueFilter.CompareOp.EQUAL,regexStringComparator);

        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        printResult(results);

    }

    /**
     * scan 过滤 分页查询
     *
     * @throws IOException
     */
    public static void pageFilterScanTest() throws IOException {
        Scan scan = new Scan();
        PageFilter pageFilter = new PageFilter(3);
        scan.setFilter(pageFilter);//设置分页

        ResultScanner results = table.getScanner(scan);
        printResult(results);
    }

    /**
     * scan 设置多个过滤
     *
     * @throws IOException
     */
    public static void filterListScanTest() throws IOException {
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE); //创建filterList，数据满足list中一个条件即可

        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("new_row_7"));
        RowFilter rowFilter = new RowFilter(RowFilter.CompareOp.EQUAL, binaryComparator);
        filterList.addFilter(rowFilter);

        RegexStringComparator regexStringComparator = new RegexStringComparator(".6$");
        ValueFilter valueFilter = new ValueFilter(ValueFilter.CompareOp.EQUAL, regexStringComparator);
        filterList.addFilter(valueFilter);

        scan.setFilter(filterList);

        ResultScanner results = table.getScanner(scan);
        printResult(results);
    }
}
