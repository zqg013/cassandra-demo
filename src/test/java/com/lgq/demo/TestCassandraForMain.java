package com.lgq.demo;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import java.io.IOException;
import java.util.Properties;

/**
 * 主线程测试Cassandra的写入操作性能
 * Created by liguoqing on 2015/4/24.
 */
public class TestCassandraForMain {

    protected static Cluster tutorialCluster;
    protected static Keyspace tutorialKeyspace;
    protected static Properties properties;

    static StringSerializer stringSerializer = StringSerializer.get();

    protected static void cassandra_init() {
        properties = new Properties();
        try {
            properties.load(TestCassandraForMain.class.getResourceAsStream("/tutorial.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        tutorialCluster = HFactory.getOrCreateCluster(properties.getProperty("cluster.name", "TutorialCluster"),
                properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
        // 数据库一致性策略
        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        // 创建keyspace
        tutorialKeyspace = HFactory.createKeyspace(properties.getProperty("tutorial.keyspace", "TestCassandra"), tutorialCluster, ccl);

    }

    public static void cassandra_insert() {
       long start = System.currentTimeMillis();
       for (int i = 0; i < 100; i++) {
           Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, stringSerializer);
           mutator.addInsertion("_people_"+i, "People_CF", HFactory.createStringColumn("first_name", "lee_"+i));
           mutator.addInsertion("_people_" + i, "People_CF", HFactory.createStringColumn("last_name", "jayden_" + i));
           mutator.addInsertion("_people_" + i, "People_CF", HFactory.createStringColumn("age", "24_" + i));
           mutator.addInsertion("_people_" + i, "People_CF", HFactory.createStringColumn("sex", "male_" + i));
           mutator.addInsertion("_people_" + i, "People_CF", HFactory.createStringColumn("address", "shanghai_" + i));
           mutator.execute();
       }
       long end = System.currentTimeMillis();
       System.out.println(end-start);

    }


    public static  void main(String[] args) {
          cassandra_init();
          cassandra_insert();
          tutorialCluster.getConnectionManager().shutdown();
    }
}
