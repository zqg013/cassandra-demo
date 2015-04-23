package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Get all the columns for a single Npanxx row.
 * Created by alone on 2015/4/23.
 */
public class GetSliceForNpanxx extends TutorialCommand {

    public GetSliceForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<ColumnSlice<String, String>> execute() {
        SliceQuery<String, String, String> sliceQuery =
                HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("Npanxx");
        sliceQuery.setKey("512202");
        // We only ever have these four columns on Npanxx
        sliceQuery.setColumnNames("city", "state", "lat", "lng");
        // The following would do the exact same as the above
        // accept here we say get the first 4 columns according to comparator order
        // sliceQuery.setRange("", "", false, 4);

        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
        return result;
    }
}
