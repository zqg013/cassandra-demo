package com.lgq.demo;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * A simple example showing what it takes to page over results using
 * get_range_slices.
 * Created by liguoqing on 2015/4/23.
 */
public class PaginateGetRangeSlices {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) throws Exception {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);

        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
        for (int i = 0; i < 20; i++) {
            mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
            .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
            .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));
        }
        mutator.execute();


        RangeSlicesQuery<String, String, String> rangeSlicesQuery =
                        HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
        rangeSlicesQuery.setColumnFamily("Standard1");
        rangeSlicesQuery.setKeys("", "");
        rangeSlicesQuery.setRange("", "", false, 3);
        rangeSlicesQuery.setRowCount(11);
        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();

        OrderedRows<String, String, String> orderedRows = result.get();
        // lastRow 第一页的最后一条记录
        Row<String,String,String> lastRow = orderedRows.peekLast();

        System.out.println("Contents of rows: \n");
        for (Row<String, String, String> r : orderedRows) {
            System.out.println("   " + r);
        }
        System.out.println("Should have 11 rows: " + orderedRows.getCount());
        // 第二次查询时，重新设置start key
        rangeSlicesQuery.setKeys(lastRow.getKey(), "");
        orderedRows = rangeSlicesQuery.execute().get();

        System.out.println("2nd page Contents of rows: \n");
        for (Row<String, String, String> row : orderedRows) {
            System.out.println("   " + row);
        }


        cluster.getConnectionManager().shutdown();

    }

}
