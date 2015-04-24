package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * A get_slice query shows how to use the comparator for ranges of text queries
 * 在指定的column family ： row key ：中查询 (切片范围 Austin - Austin__204) 列名的范围
 * Created by alone on 2015/4/23.
 */
public class GetSliceForAreaCodeCity extends TutorialCommand {

    public GetSliceForAreaCodeCity(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<String, String>> execute() {
        SliceQuery<String, String, String> sliceQuery =
                HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("AreaCode");
        sliceQuery.setKey("512");
        // change the order argument to 'true' to get the last 2 columns in descending order
        // gets the first 4 columns "between" Austin and Austin__204 according to comparator
        // start key ： end key ： 顺序/逆序 ： 条数
        sliceQuery.setRange("Austin__", "Austin__204", false, 5);

        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();

        return result;
    }

}
