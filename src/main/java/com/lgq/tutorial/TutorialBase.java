package com.lgq.tutorial;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by liguoqing on 2015/4/23.
 */
public class TutorialBase {

    protected static Cluster tutorialCluster;
    protected static Keyspace tutorialKeyspace;
    protected static Properties properties;

    protected static void init() {

        properties = new Properties();
        try {
            properties.load(TutorialBase.class.getResourceAsStream("/tutorial.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // To modify the default ConsistencyLevel of QUORUM, create a
        // me.prettyprint.hector.api.ConsistencyLevelPolicy and use the overloaded form:
        // tutorialKeyspace = HFactory.createKeyspace("Tutorial", tutorialCluster, consistencyLevelPolicy);
        // see also me.prettyprint.cassandra.model.ConfigurableConsistencyLevelPolicy[Test] for details

        tutorialCluster = HFactory.getOrCreateCluster(properties.getProperty("cluster.name", "TutorialCluster"),
                            properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
        // 数据库一致性策略
        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        // 创建keyspace
        tutorialKeyspace = HFactory.createKeyspace(properties.getProperty("tutorial.keyspace", "Tutorial"), tutorialCluster, ccl);

    }

}
