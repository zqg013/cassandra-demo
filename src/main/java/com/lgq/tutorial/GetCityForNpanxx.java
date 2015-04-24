package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Get a single column, 'City', for a specific NPA/NXX combination.
 * 根据指定的ColumnFamily - row_key - column_name获取某一列的值
 * Created by liguoqing on 2015/4/23.
 */
public class GetCityForNpanxx extends TutorialCommand {

    public GetCityForNpanxx(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<HColumn<String, String>> execute() {

        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspace);
        columnQuery.setColumnFamily("Npanxx");
        columnQuery.setKey("512204");
        columnQuery.setName("city");
        QueryResult<HColumn<String, String>> result = columnQuery.execute();

        return result;
    }

}
