package datawave.ingest.table.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;

public interface TableConfigHelper {
    
    String TABLE_CONFIG_CLASS_SUFFIX = ".table.config.class";
    String TABLE_CONFIG_PREFIX = ".table.config.prefix";
    
    /**
     * Performs property validation and setup
     * 
     * @param config
     * @throws IllegalArgumentException
     */
    void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException;
    
    /**
     * Configures table
     * 
     * @param tops
     *            Accumulo TableOperations object to use to setup the configuration
     * 
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     * 
     */
    void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
    
}
