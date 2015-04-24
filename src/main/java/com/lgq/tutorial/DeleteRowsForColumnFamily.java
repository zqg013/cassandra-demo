package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Uses batch_mutate to delete the rows created by {@link InsertRowsForColumnFamilies}
 * <p/>
 * This shows an example of range ghosts post deletion. For more information, see:
 * http://wiki.apache.org/cassandra/FAQ#range_ghosts
 * http://wiki.apache.org/cassandra/DistributedDeletes
 *
 * 删除数据，是将 column 删除掉了，row key 还是存在的
 * @author liguoqing
 */
public class DeleteRowsForColumnFamily extends TutorialCommand {

    public DeleteRowsForColumnFamily(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<?> execute() {
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

        mutator.addDeletion("CA Burlingame", "StateCity", null, stringSerializer);
        mutator.addDeletion("650", "AreaCode", null, stringSerializer);
        mutator.addDeletion("650222", "Npanxx", null, stringSerializer);
        // adding a non-existent key like the following will cause the insertion of a tombstone
        // mutator.addDeletion("652", "AreaCode", null, stringSerializer);
        MutationResult mr = mutator.execute();
        return null;

    }

}