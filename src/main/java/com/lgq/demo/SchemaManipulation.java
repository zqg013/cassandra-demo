package com.lgq.demo;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.factory.HFactory;
import java.util.Arrays;
import java.util.List;

/**
 * Creates a keyspace and adds a column family. This column family
 * contains an index on the column named 'birthdate' which is exptected
 * to be a Long.
 * Created by liguoqing on 2015/4/22.
 */
public class SchemaManipulation {

    private static final String DYN_KEYSPACE = "DynamicKeyspace";
    private static final String DYN_CF = "DynamicCf";
    private static final String CF_SUPER = "SuperCf";

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        // 判断是否存在 KeyspaceDefinition
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(DYN_KEYSPACE);
        if (keyspaceDef != null) {
            cluster.dropKeyspace(DYN_KEYSPACE);
        }

        // 定义column
        BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
        columnDefinition.setName(stringSerializer.toByteBuffer("birth_date"));
        columnDefinition.setIndexName("birthdate_idx");
        columnDefinition.setIndexType(ColumnIndexType.KEYS);
        columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());

        // 定义columnfamily
        BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
        columnFamilyDefinition.setKeyspaceName(DYN_KEYSPACE);
        columnFamilyDefinition.setName(DYN_CF);
        columnFamilyDefinition.addColumnDefinition(columnDefinition);

        BasicColumnFamilyDefinition superCfDefinition = new BasicColumnFamilyDefinition();
        superCfDefinition.setKeyspaceName(DYN_KEYSPACE);
        superCfDefinition.setName(CF_SUPER);
        superCfDefinition.setColumnType(ColumnType.SUPER);

        ColumnFamilyDefinition cfDefStandard = new ThriftCfDef(columnFamilyDefinition);
        ColumnFamilyDefinition cfDefSuper = new ThriftCfDef(superCfDefinition);


        KeyspaceDefinition keyspaceDefinition =
                HFactory.createKeyspaceDefinition(DYN_KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy",
                        1, Arrays.asList(cfDefStandard, cfDefSuper));

        cluster.addKeyspace(keyspaceDefinition);

        // insert some data
        List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
        for (KeyspaceDefinition kd : keyspaces) {
            if ( kd.getName().equals(DYN_KEYSPACE) ) {
                System.out.println("Name: " +kd.getName());
                System.out.println("RF: " +kd.getReplicationFactor());
                System.out.println("strategy class: " +kd.getStrategyClass());
                List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
                for (ColumnFamilyDefinition def : cfDefs) {
                  System.out.println("  CF Type: " +def.getColumnType());
                  System.out.println("  CF Name: " +def.getName());
                  System.out.println("  CF Metadata: " +def.getColumnMetadata());
                }
            }
        }


        cluster.getConnectionManager().shutdown();




    }

}
