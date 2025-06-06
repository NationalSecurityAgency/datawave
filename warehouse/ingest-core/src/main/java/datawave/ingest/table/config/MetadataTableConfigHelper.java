package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.data.ColumnFamilyConstants;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.iterators.FrequencyMetadataAggregator;

public class MetadataTableConfigHelper extends AbstractTableConfigHelper {

    protected Logger log;
    protected Configuration conf;
    protected String tableName;

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (tableName != null) {
            for (IteratorScope scope : IteratorScope.values()) {
                setFrequencyAggregator(tops, scope.name());
                setCombinerForCountMetadata(tops, scope.name());
                setCombinerForEdgeMetadata(tops, scope.name());
            }
        }

    }

    // Add the EdgeMetadataCombiner to the edge column.
    private String setCombinerForEdgeMetadata(TableOperations tops, String scopeName)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "EdgeMetadataCombiner");
        setPropertyIfNecessary(tableName, stem, "19,datawave.iterators.EdgeMetadataCombiner", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns", ColumnFamilyConstants.COLF_EDGE.toString(), tops, log);
        return stem;
    }

    // Add the CountMetadataCombiner to the count column.
    private String setCombinerForCountMetadata(TableOperations tops, String scopeName)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "CountMetadataCombiner");
        setPropertyIfNecessary(tableName, stem, "15,datawave.iterators.CountMetadataCombiner", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns", ColumnFamilyConstants.COLF_COUNT.toString(), tops, log);
        return stem;
    }

    private String setFrequencyAggregator(TableOperations tops, String scopeName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "FrequencyAggregator");
        setPropertyIfNecessary(tableName, stem, "13," + FrequencyMetadataAggregator.class.getName(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns",
                        ColumnFamilyConstants.COLF_F.toString() + ',' + ColumnFamilyConstants.COLF_I + ',' + ColumnFamilyConstants.COLF_RI, tops, log);
        return stem;
    }

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {

        this.log = log;
        this.conf = config;
        this.tableName = conf.get(ShardedDataTypeHandler.METADATA_TABLE_NAME, null);

        if (this.tableName == null || !this.tableName.equals(tableName)) {
            throw new IllegalArgumentException("No Such Table: " + tableName);
        }

    }

}
