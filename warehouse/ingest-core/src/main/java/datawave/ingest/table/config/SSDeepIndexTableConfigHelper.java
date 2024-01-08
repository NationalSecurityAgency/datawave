package datawave.ingest.table.config;

import datawave.ingest.mapreduce.handler.facet.FacetHandler;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

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
        
        ssdeepIndexTableName = conf.get(SSDeepIndexHandler.SSDEEP_INDEX_TABLE_NAME, null);

        
        if (ssdeepIndexTableName == null) {
            throw new IllegalArgumentException("No Facet Table names are defined");
        }
        
        // TODO: generic balancer, markings, bloom filters, locality groups.
        
        if (tableName.equals(ssdeepIndexTableName)) {
            this.tableType = SSDeepTableType.INDEX;
        } else {
            throw new IllegalArgumentException("Invalid Facet Table Definition Fot: " + tableName);
        }
        this.tableName = tableName;
    }
    
    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        switch (this.tableType) {
            case INDEX:
                configureSSDeepIndexTable(tops);
                break;
            default:
                // Technically, this is dead code. If 'Configure' is called prior to 'Setup'
                // tableType is null and throws a NullPointerException in the switch statement.
                // If 'Setup' successfully runs to completion then tableType is assigned one
                // of the three other values.
                throw new TableNotFoundException(null, tableName, "Table is not a SSDeep Type Table");
        }
    }
    
    protected void configureSSDeepIndexTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // TODO:
    }
}
