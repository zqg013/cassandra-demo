package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Like {@link GetSliceForNpanxx}, but uses the multiget_slice to
 * apply the slice operation to mulitple keys
 * 在column family中 指定多个row key 进行查询 多个column
 * Created by alone on 2015/4/23.
 */
public class MultigetSliceForNpanxx extends TutorialCommand {

    public MultigetSliceForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<Rows<String, String, String>> execute() {
        MultigetSliceQuery<String, String, String> multigetSlicesQuery =
                HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        multigetSlicesQuery.setColumnFamily("Npanxx");
        multigetSlicesQuery.setColumnNames("city", "state", "lat", "lng");
        multigetSlicesQuery.setKeys("512202", "512203", "512205", "512206");
        QueryResult<Rows<String, String, String>> results = multigetSlicesQuery.execute();
        return results;
    }

}
