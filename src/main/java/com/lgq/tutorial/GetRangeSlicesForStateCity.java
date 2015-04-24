package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * Get all the columns for a "range" of Npanxx rows. Range in this sense
 * is only meaningful if you are using OrderPreservingPartitioner. With
 * RandomPartitioner, the default, you are getting the token range (converted
 * from hashed keys).
 * For more information, see: http://wiki.apache.org/cassandra/FAQ#range_rp
 * KeyRange: http://wiki.apache.org/cassandra/API#KeyRange
 *
 *  在column family中 查询 指定 row key 范围 的所有列(或指定列)的值
 *
 * Created by liguoqing on 2015/4/23.
 */
public class GetRangeSlicesForStateCity extends TutorialCommand {


    public GetRangeSlicesForStateCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<OrderedRows<String, String, String>> execute() {
        RangeSlicesQuery<String, String, String> rangeSlicesQuery =
                HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        rangeSlicesQuery.setColumnFamily("Npanxx");
        rangeSlicesQuery.setColumnNames("city", "state", "lat", "lng");
        rangeSlicesQuery.setKeys("512202", "512205");  // 开始row key - 结束row key
        rangeSlicesQuery.setRowCount(5);
        QueryResult<OrderedRows<String, String, String>> results = rangeSlicesQuery.execute();
        return results;
    }
}
