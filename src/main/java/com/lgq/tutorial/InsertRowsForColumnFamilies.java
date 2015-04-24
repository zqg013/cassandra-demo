package com.lgq.tutorial;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Uses batch_mutate to insert several rows into mutliple column families in
 * one execution.
 *
 * @author liguoqing
 */
public class InsertRowsForColumnFamilies extends TutorialCommand {

    public InsertRowsForColumnFamilies(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public QueryResult<?> execute() {

        boolean batch = true;
        if(batch) {
            System.out.println("------------start------------------");
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
                mutator.addInsertion("people_"+i, "Npanxx", HFactory.createStringColumn("firs_name", "lee_"+i));
                mutator.addInsertion("people_"+i, "Npanxx", HFactory.createStringColumn("last_name", "jayden_"+i));
                mutator.addInsertion("people_"+i, "Npanxx", HFactory.createStringColumn("age", "24_"+i));
                mutator.addInsertion("people_"+i, "Npanxx", HFactory.createStringColumn("sex", "male_"+i));
                mutator.addInsertion("people_"+i, "Npanxx", HFactory.createStringColumn("address", "shanghai_"+i));
                MutationResult mr = mutator.execute();
            }
            long end = System.currentTimeMillis();
            System.out.println(end-start);
            System.out.println("------------end------------------");
        } else {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            mutator.addInsertion("CA Burlingame", "StateCity", HFactory.createColumn(650L, "37.57x122.34", longSerializer, stringSerializer));
            mutator.addInsertion("650", "AreaCode", HFactory.createStringColumn("Burlingame__650", "37.57x122.34"));
            mutator.addInsertion("650223", "Npanxx", HFactory.createStringColumn("lat", "37.57"));
            mutator.addInsertion("650223", "Npanxx", HFactory.createStringColumn("lng", "122.34"));
            mutator.addInsertion("650223", "Npanxx", HFactory.createStringColumn("city", "beijing"));
            mutator.addInsertion("650223", "Npanxx", HFactory.createStringColumn("state", "china"));
            MutationResult mr = mutator.execute();
        }

        return null;
    }

}