package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 *  A get_slice call that shows demonstrates how to take advantage of the
 * ordering provided by a column families comparator.
 * Created by alone on 2015/4/23.
 */
public class GetSliceForStateCity extends TutorialCommand {

    public GetSliceForStateCity(Keyspace keyspace) {
            super(keyspace);
        }

        @Override
        public QueryResult<ColumnSlice<Long,String>> execute() {
            SliceQuery<String, Long, String> sliceQuery =
                HFactory.createSliceQuery(keyspace, stringSerializer, longSerializer, stringSerializer);
            sliceQuery.setColumnFamily("StateCity");
            sliceQuery.setKey("TX Austin");
            // change 'reversed' to true to get the columns in reverse order
            // reversed 改为 true ，范围应该更改为 end key - start key
            sliceQuery.setRange(202L, 204L, false, 5);
            QueryResult<ColumnSlice<Long, String>> result = sliceQuery.execute();
            return result;
        }
}
