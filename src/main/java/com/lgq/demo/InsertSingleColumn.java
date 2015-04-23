package com.lgq.demo;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Inserts the value "John" under the Column "first" for the
 * key "jsmith" in the Standard1 ColumnFamily
 * Created by liguoqing on 2015/4/23.
 */
public class InsertSingleColumn {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);

        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

        // 插入一行记录
        mutator.insert("jsmith", "Standard1", HFactory.createStringColumn("first", "John"));

        // 单行查询
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
        columnQuery.setColumnFamily("Standard1").setKey("jsmith").setName("first");
        QueryResult<HColumn<String, String>> result = columnQuery.execute();

        // result.get() : 获取result的值
        System.out.println("Read HColumn from cassandra: " + result.get());
        System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");

        cluster.getConnectionManager().shutdown();
    }
}
