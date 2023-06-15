package datawave.metrics.util;

import datawave.ingest.table.config.AbstractTableConfigHelper;
import datawave.metrics.config.MetricsConfig;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.LinkedList;

public class Connections {
    private static final Logger log = Logger.getLogger(Connections.class);
    private static final String AGE_OFF = "ageoff";

    /**
     * Utility method that will initialize all tables if they are not currently present.
     *
     * @param conf
     *            a configuration
     * @throws AccumuloSecurityException
     *             if there is a problem with accumulo authentication
     * @throws AccumuloException
     *             if there is a general issue with accumulo
     *
     */
    public static void initTables(Configuration conf) throws AccumuloException, AccumuloSecurityException {
        LinkedList<String> tables = new LinkedList<>();
        tables.add(conf.get(MetricsConfig.METRICS_TABLE, MetricsConfig.DEFAULT_METRICS_TABLE));
        tables.add(conf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE));
        tables.add(conf.get(MetricsConfig.LOADER_TABLE, MetricsConfig.DEFAULT_LOADER_TABLE));
        tables.add(conf.get(MetricsConfig.FILE_GRAPH_TABLE, MetricsConfig.DEFAULT_FILE_GRAPH_TABLE));
        tables.add(conf.get(MetricsConfig.METRICS_SUMMARY_TABLE, MetricsConfig.DEFAULT_METRICS_SUMMARY_TABLE));
        tables.add(conf.get(MetricsConfig.BAD_SELECTOR_TABLE, MetricsConfig.DEFAULT_BAD_SELECTOR_TABLE));
        String hourlyTableName = conf.get(MetricsConfig.METRICS_HOURLY_SUMMARY_TABLE, MetricsConfig.DEFAULT_HOURLY_METRICS_SUMMARY_TABLE);

        try (AccumuloClient client = metricsClient(conf)) {
            TableOperations tops = client.tableOperations();
            for (String table : tables) {
                if (!tops.exists(table)) {
                    createTable(tops, table);
                }
            }

            if (!tops.exists(hourlyTableName)) {
                createTable(tops, hourlyTableName);
                configureClasspathContext(tops, hourlyTableName);
                configureAgeOff(tops, hourlyTableName, 30);
            }
        }

        try (AccumuloClient client = warehouseClient(conf)) {
            TableOperations tops = client.tableOperations();

            tables = new LinkedList<>();
            tables.add(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE));

            for (String table : tables) {
                if (!tops.exists(table)) {
                    createTable(tops, table);
                }
            }
        }
    }

    private static void configureClasspathContext(TableOperations tops, String tableName) {
        try {
            AbstractTableConfigHelper.setPropertyIfNecessary(tableName, "table.classpath.context", "datawave", tops, log);
        } catch (Exception e) {
            log.error("Exception occurred when attempting to set the table classpath context", e);
        }
    }

    private static void configureAgeOff(TableOperations tops, String tableName, int numDays) {
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope, AGE_OFF);
            try {
                tops.setProperty(tableName, stem + ".opt.ttl", "45");
                tops.setProperty(tableName, stem, "18,datawave.iterators.filter.DateInRowAgeOffFilter");
            } catch (AccumuloException | AccumuloSecurityException e) {
                log.error("Could not create age off configuration for " + stem, e);
            }
        }
    }

    private static void createTable(TableOperations tops, String table) {
        try {
            tops.create(table);
        } catch (Exception e) {
            log.error("Could not create table " + table, e);
        }
    }

    public static AccumuloClient metricsClient(Configuration c) throws AccumuloException, AccumuloSecurityException {
        final String mtxZk = c.get(MetricsConfig.ZOOKEEPERS), mtxInst = c.get(MetricsConfig.INSTANCE), mtxUser = c.get(MetricsConfig.USER),
                        mtxPass = c.get(MetricsConfig.PASS);
        return Accumulo.newClient().to(mtxInst, mtxZk).as(mtxUser, mtxPass).build();
    }

    public static AccumuloClient warehouseClient(Configuration c) throws AccumuloException, AccumuloSecurityException {
        final String whZk = c.get(MetricsConfig.WAREHOUSE_ZOOKEEPERS), whInst = c.get(MetricsConfig.WAREHOUSE_INSTANCE),
                        whUser = c.get(MetricsConfig.WAREHOUSE_USERNAME), whPass = c.get(MetricsConfig.WAREHOUSE_PASSWORD);
        return Accumulo.newClient().to(whInst, whZk).as(whUser, whPass).build();
    }
}
