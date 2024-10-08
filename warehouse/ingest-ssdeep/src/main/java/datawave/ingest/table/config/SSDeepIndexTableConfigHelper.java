package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;

public class SSDeepIndexTableConfigHelper extends AbstractTableConfigHelper {

    protected Logger log;

    public enum SSDeepTableType {
        INDEX
    }

    protected Configuration conf;
    protected String tableName;
    protected String ssdeepIndexTableName;
    protected SSDeepTableType tableType;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        this.log = log;
        this.conf = config;

        ssdeepIndexTableName = conf.get(SSDeepIndexHandler.SSDEEP_INDEX_TABLE_NAME, SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME);

        // TODO: generic balancer, markings, bloom filters, locality groups.

        if (tableName.equals(ssdeepIndexTableName)) {
            this.tableType = SSDeepTableType.INDEX;
        } else {
            throw new IllegalArgumentException("Invalid SSDeepIndex Table Definition For: " + tableName);
        }
        this.tableName = tableName;
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (this.tableType == SSDeepTableType.INDEX) {
            configureSSDeepIndexTable(tops);
            return;
        }

        throw new TableNotFoundException(null, tableName, "Table is not a SSDeep Index Type Table");
    }

    @SuppressWarnings({"unused", "RedundantThrows"})
    protected void configureSSDeepIndexTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        /* currently a no-op for test use, will potentially need to implement this for production use */
    }
}
