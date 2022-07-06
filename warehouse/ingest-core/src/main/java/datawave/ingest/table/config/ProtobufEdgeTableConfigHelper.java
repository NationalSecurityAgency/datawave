package datawave.ingest.table.config;

import datawave.ingest.mapreduce.handler.edge.ProtobufEdgeDataTypeHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public class ProtobufEdgeTableConfigHelper extends AbstractTableConfigHelper {
    private static final int DEFAULT_VERSIONING_ITERATOR_PRIORITY = 20;
    private Logger log;
    
    private Configuration conf;
    protected String tableName;
    private String priority;
    
    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        
        if (tableName != null) {
            // Add the Edge Combiner
            for (IteratorScope scope : IteratorScope.values()) {
                // Add the EdgeCombiner BELOW the Versioning iterator
                int combinerPriority = getVersionIteratorPriority(tops, scope) - 1;
                String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "EdgeCombiner");
                setPropertyIfNecessary(tableName, stem, combinerPriority + ",datawave.iterators.EdgeCombiner", tops, log);
                setPropertyIfNecessary(tableName, stem + ".opt.all", "true", tops, log);
                
            }
        }
    }
    
    /**
     * FInds the priority associated with versioning iterator. Assumes the name is "vers".
     * 
     * @param tops
     * @param scope
     *            The scope of the iterator.
     * @return The priority of the versioning iterator, or 20 if not found.
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    private int getVersionIteratorPriority(final TableOperations tops, final IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
                    TableNotFoundException {
        final IteratorSetting versioningIterator = tops.getIteratorSetting(tableName, "vers", scope);
        
        return ((null == versioningIterator) ? DEFAULT_VERSIONING_ITERATOR_PRIORITY : versioningIterator.getPriority());
    }
    
    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        
        this.log = log;
        this.conf = config;
        this.tableName = conf.get(ProtobufEdgeDataTypeHandler.EDGE_TABLE_NAME, null);
        this.priority = conf.get(ProtobufEdgeDataTypeHandler.EDGE_TABLE_LOADER_PRIORITY, null);
        
        if (this.tableName == null || !this.tableName.equals(tableName) || this.priority == null) {
            throw new IllegalArgumentException("Edge Table Not Properly Defined: " + tableName);
        }
        
    }
    
}
