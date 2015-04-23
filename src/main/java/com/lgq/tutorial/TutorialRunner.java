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

        TutorialCommand command = loadCommand(args[0]);
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
        if (cmd.equalsIgnoreCase("get")) {
            return new GetCityForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice")) {
            return new GetSliceForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_range_slices")) {
            return new GetRangeSlicesForStateCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice_acc")) {
            return new GetSliceForAreaCodeCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_slice_sc")) {
            return new GetSliceForStateCity(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("multiget_slice")) {
            return new MultigetSliceForNpanxx(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_indexed_slices")) {
            return new GetIndexedSlicesForCityState(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_indexed_slices_raw")) {
            return new GetIndexedSlicesHandleRawBytes(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("insert")) {
            return new InsertRowsForColumnFamilies(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("delete")) {
            return new DeleteRowsForColumnFamily(tutorialKeyspace);
        } else if (cmd.equalsIgnoreCase("get_hcol")) {
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
