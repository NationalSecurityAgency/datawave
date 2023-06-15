package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public interface TableConfigHelper {

    String TABLE_CONFIG_CLASS_SUFFIX = ".table.config.class";
    String TABLE_CONFIG_PREFIX = ".table.config.prefix";

    /**
     * Performs property validation and setup
     *
     * @param tableName
     *            the table name
     * @param log
     *            the log
     * @param config
     *            a configuration
     * @throws IllegalArgumentException
     *             if there is an issue with an argument
     */
    void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException;

    /**
     * Configures table
     *
     * @param tops
     *            Accumulo TableOperations object to use to setup the configuration
     *
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws TableNotFoundException
     *             if the table is not found
     *
     */
    void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

}
