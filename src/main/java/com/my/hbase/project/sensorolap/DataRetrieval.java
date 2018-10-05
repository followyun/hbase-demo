package com.my.hbase.project.sensorolap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 数据检索
 * 检索零部件NE-114上发送了ALERT类型事件的传感器发出的其他的时间
 * solr的依赖
 * <dependency>
 * <groupId>org.apache.solr</groupId>
 * <artifactId>solr-solrj</artifactId>
 * <version>${solr.version}</version>
 * <type>jar</type>
 * </dependency>
 */
public class DataRetrieval {

    public static void main(String[] args) throws IOException, SolrServerException {
        CloudSolrClient solrClient = new CloudSolrClient.Builder()
                .withZkHost("master:2181,slave1:2181,slave2:2181")
                .withZkChroot("/solr").build();
        solrClient.connect();
        solrClient.setDefaultCollection("sensor");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "select");
        params.set("q", "+eventType:ALERT +partName:NE-114");

        QueryResponse response = solrClient.query(params);
        SolrDocumentList docs = response.getResults();

        if (docs.getNumFound() == 0) {
            System.out.println("查询结果数为：" + docs.getNumFound());
            System.exit(0);
        }

        List<Get> gets = new ArrayList<>();
        docs.forEach(new Consumer<SolrDocument>() {
            @Override
            public void accept(SolrDocument entries) {
                gets.add(new Get((byte[]) entries.getFieldValue("rowkey")));
            }
        });
        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf("sensor"))) {
            Result[] results = table.get(gets);
            for (Result result :
                    results) {
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
            }
        }

        System.out.println("获取数据成功！");

    }


}
