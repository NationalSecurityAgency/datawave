package datawave.ingest.mapreduce.job.metrics;

import java.util.Collection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.table.config.AbstractTableConfigHelper;

/**
 * Table configuration helper for the ingest metrics table.
 */
public class MetricsTableConfigHelper extends AbstractTableConfigHelper {

    private String tableName;
    private Configuration conf;
    private Logger logger;

    @Override
    public void setup(String tableName, Configuration conf, Logger logger) throws IllegalArgumentException {
        this.conf = conf;
        this.tableName = tableName;
        this.logger = logger;
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (MetricsConfiguration.isEnabled(conf)) {
            try {
                String table = MetricsConfiguration.getTable(conf);

                if (!table.equals(this.tableName)) {
                    throw new IllegalArgumentException("Table names did not match. Configuration = " + table + ", Configuration Helper = " + this.tableName);
                }

                Collection<MetricsReceiver> receivers = MetricsConfiguration.getReceivers(conf);
                for (MetricsReceiver receiver : receivers) {
                    logger.info("Configuring metrics receiver " + receiver);
                    receiver.configureTable(table, tops, conf);
                }
            } catch (Exception e) {
                logger.error("An error occurred while configuring ingest metrics, disabling", e);
                MetricsConfiguration.disable(conf);
            }
        }
    }
}
