package com.lgq.demo;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * Shows off the new ExecutionResult hierarchy
 * Created by liguoqing on 2015/4/23.
 */
public class ResultDetailsDemo {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) throws Exception {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);

        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

        // add 10 rows 插入10行记录
        for (int i = 0; i < 10; i++) {
            // Standard1 代表column family
            mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
            .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
            .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));
        }
        MutationResult me = mutator.execute();
        System.out.println("MutationResult from 10 row insertion: " + me);

        // rangeSlices 查询
        RangeSlicesQuery<String, String, String> rangeSlicesQuery =
                        HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
        rangeSlicesQuery.setColumnFamily("Standard1");
        rangeSlicesQuery.setKeys("", "");
        rangeSlicesQuery.setRange("", "", false, 3);
        rangeSlicesQuery.setRowCount(5);
        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
        System.out.println("Result from rangeSlices query: " + result.toString());

        // column  查询
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
         // 在column family 中指定 row key 和column name进行查找
        columnQuery.setColumnFamily("Standard1").setKey("fake_key_0").setName("fake_column_0");
        QueryResult<HColumn<String, String>> colResult = columnQuery.execute();
        System.out.println("Execution time: " + colResult.getExecutionTimeMicro());
        System.out.println("CassandraHost used: " + colResult.getHostUsed());
        System.out.println("Query Execute: " + colResult.getQuery());

        cluster.getConnectionManager().shutdown();


    }
}
