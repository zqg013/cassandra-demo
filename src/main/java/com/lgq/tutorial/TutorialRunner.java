package com.lgq.tutorial;

import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Created by liguoqing on 2015/4/23.
 */
public class TutorialRunner extends TutorialBase {

    private static Logger log = LoggerFactory.getLogger(TutorialRunner.class);


    public static void main(String[] args) {

        init();

        TutorialCommand command = loadCommand("insert");
        if (command != null) {
            try {
                ResultStatus result = command.execute();
                if (result != null)
                    printResults(result);

            } catch (Exception e) {
                // Cow catcher. Feel free to explore exception types here.
                // Most everything should be wrapped in a HectorException
                log.error("Problem executing command:", e);
            }
        }
        // NOTE: you can comment out this line to leave the JVM running.
        // This will allow you to look at JMX statistics of what you just
        // did and get a feel for Hector's JMX integration.
        tutorialCluster.getConnectionManager().shutdown();
    }


    @SuppressWarnings("unchecked")
    private static void printResults(ResultStatus result) {
        log.info("+-------------------------------------------------");
        log.info("| Result executed in: {} microseconds against host: {}",
                result.getExecutionTimeMicro(), result.getHostUsed().getName());
        log.info("+-------------------------------------------------");
        // nicer display of Rows vs. HColumn or ColumnSlice
        if (result instanceof QueryResult) {
            System.out.println(((QueryResult) result).get());
            QueryResult<?> qr = (QueryResult) result;
            if (qr.get() instanceof Rows) {

                Rows<?, ?, ?> rows = (Rows) qr.get();

                for (Row row : rows) {
                    log.info("| Row key: {}", row.getKey());
                    for (Iterator iter = row.getColumnSlice().getColumns().iterator(); iter.hasNext(); ) {
                        log.info("|   col: {}", iter.next());
                    }

                }
            } else if (qr.get() instanceof ColumnSlice) {
                for (Iterator iter = ((ColumnSlice) qr.get()).getColumns().iterator(); iter.hasNext(); ) {
                    log.info("|   col: {}", iter.next());
                }
            } else {
                log.info("| Result: {}", qr.get());
            }
        }
        log.info("+-------------------------------------------------");
    }

    /*
     * Simple command lookup based on string. Returns null on a miss.
     * Would be nice to have something fancier with enums at some point.
     */
    private static TutorialCommand loadCommand(String cmd) {
        if (cmd.equalsIgnoreCase("get")) {                  //  根据指定的ColumnFamily - row_key - column_name获取某一列的值
            return new GetCityForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice")) {       // 在column family中 查询 指定 row key 的所有列(或指定列)的值
            return new GetSliceForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_range_slices")) {  //  在column family中 查询 指定 row key 范围 的所有列(或指定列)的值
            return new GetRangeSlicesForStateCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice_acc")) {  //   在指定的column family ： row key ：中查询 (切片范围 Austin - Austin__204) 列名的范围
            return new GetSliceForAreaCodeCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice_sc")) {    // 同上
            return new GetSliceForStateCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("multiget_slice")) {   // 在column family中 指定多个row key 进行查询 多个column
            return new MultigetSliceForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_indexed_slices")) {   // 在column family中根据 column value的条件进行查询
            return new GetIndexedSlicesForCityState(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_indexed_slices_raw")) {  // 同上
            return new GetIndexedSlicesHandleRawBytes(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("insert")) {    // 插入数据
            return new InsertRowsForColumnFamilies(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("delete")) {     // 删除数据
            return new DeleteRowsForColumnFamily(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_hcol")) {       // 使用HColumnFamily 获取column
            return new GetNpanxxHColumnFamily(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("static_comp_index")) {
            return new StaticCompositeIndex(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("dynamic_comp_index")) {
            return new DynamicCompositeIndex(tutorialKeyspace);
        }
        log.error(" ***OOPS! No match found for {}.", cmd);
        return null;
    }
}
