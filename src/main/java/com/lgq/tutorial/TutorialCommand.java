package com.lgq.tutorial;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for individual tutorial classes to implement.
 * Created by liguoqing on 2015/4/23.
 */
public abstract class TutorialCommand {

    protected Logger log = LoggerFactory.getLogger(TutorialCommand.class);

    static StringSerializer stringSerializer = StringSerializer.get();

    static LongSerializer longSerializer = LongSerializer.get();

    protected Keyspace keyspace;

    public TutorialCommand(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * To be implemented by tutorial classes.
     *
     * @return QueryResult which is typed quite differently
     * depending on the implementation
     */
    public abstract ResultStatus execute();



}
