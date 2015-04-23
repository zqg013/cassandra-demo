package com.lgq.demo;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import java.util.Arrays;

/**
 * Like {@link InsertSingleColumn} except with a SuperColumn
 * Created by alone on 2015/4/23.
 */
public class InsertSuperColumn {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);

        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

        mutator.insert("billing", "Super1", HFactory.createSuperColumn("bob",
                Arrays.asList(HFactory.createStringColumn("first", "wu")),
                stringSerializer, stringSerializer, stringSerializer));

        SuperColumnQuery<String, String, String, String> superColumnQuery =
                HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer,
                        stringSerializer, stringSerializer);
        superColumnQuery.setColumnFamily("Super1").setKey("billing").setSuperName("bob");

        QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();

        System.out.println("Read HSuperColumn from cassandra: " + result.get());
        System.out.println("Verify on CLI with:  get Keyspace1.Super1['billing']['jsmith'] ");

        cluster.getConnectionManager().shutdown();

    }
}
