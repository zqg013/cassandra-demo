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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程环境下，测试Cassandra的写入操作性能
 * Created by liguoqing on 2015/4/24.
 */
public class TestCassandraForThread {

    protected static Cluster tutorialCluster;
    protected static Keyspace tutorialKeyspace;
    protected static Properties properties;
    static StringSerializer stringSerializer = StringSerializer.get();

    private String threadName;

    public TestCassandraForThread(){}

    public TestCassandraForThread(String threadName) {
        this.threadName = threadName;
    }

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

    public static void main(String[] args) {

        cassandra_init();

        int count = 3000; //  线程数目
        ExecutorService executorService = Executors.newFixedThreadPool(count);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            executorService.execute(new TestCassandraForThread("thread"+i).new Task());
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("end====================>"+(end-start));
    }

    public class Task implements Runnable {

        @Override
        public void run() {
            try {
                // 测试内容
                for (int i = 0; i < 1; i++) {
                    System.out.println(threadName + "==========run==============" + i);
                    Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, stringSerializer);
                    mutator.addInsertion(threadName+"_people_"+i, "People_CF", HFactory.createStringColumn("first_name", "lee_"+i));
                    mutator.addInsertion(threadName+"_people_" + i, "People_CF", HFactory.createStringColumn("last_name", "jayden_" + i));
                    mutator.addInsertion(threadName+"_people_" + i, "People_CF", HFactory.createStringColumn("age", "24_" + i));
                    mutator.addInsertion(threadName+"_people_" + i, "People_CF", HFactory.createStringColumn("sex", "male_" + i));
                    mutator.addInsertion(threadName+"_people_" + i, "People_CF", HFactory.createStringColumn("address", "shanghai_" + i));
                    mutator.execute();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
