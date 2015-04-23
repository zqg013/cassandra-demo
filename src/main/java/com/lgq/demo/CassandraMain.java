package com.lgq.demo;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.Arrays;

/**
 * Created by liguoqing on 2015/4/22.
 */
public class CassandraMain {

    public static void main(String[] args) {

//        Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
//
//        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeySpace", "ColumnFamilyName", ComparatorType.BYTESTYPE);
//
//        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeySpace", ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
//
//        cluster.addKeyspace(newKeyspace, true);


    }
}
