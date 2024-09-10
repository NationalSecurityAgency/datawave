package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.data.ColumnFamilyConstants;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

public class MetadataTableConfigHelper extends AbstractTableConfigHelper {

    protected Logger log;
    protected Configuration conf;
    protected String tableName;

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (tableName != null) {
            for (IteratorScope scope : IteratorScope.values()) {
                setFrequencyCombiner(tops, scope.name());
                setIndexCombiner(tops, scope.name());
                setReverseIndexCombiner(tops, scope.name());
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

    // Add the SummingCombiner to the frequency column.
    private String setFrequencyCombiner(TableOperations tops, String scopeName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "FrequencyCombiner");
        setPropertyIfNecessary(tableName, stem, "10," + SummingCombiner.class.getName(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns", ColumnFamilyConstants.COLF_F.toString(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.type", "VARLEN", tops, log);
        return stem;
    }

    // Add the SummingCombiner to the indexed column.
    private String setIndexCombiner(TableOperations tops, String scopeName) throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "IndexCombiner");
        setPropertyIfNecessary(tableName, stem, "11," + SummingCombiner.class.getName(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns", ColumnFamilyConstants.COLF_I.toString(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.lossy", "true", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.type", "VARLEN", tops, log);
        return stem;
    }

    // Add the SummingCombiner to the reverse indexed column.
    private String setReverseIndexCombiner(TableOperations tops, String scopeName) throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "ReverseIndexCombiner");
        setPropertyIfNecessary(tableName, stem, "12," + SummingCombiner.class.getName(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.columns", ColumnFamilyConstants.COLF_RI.toString(), tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.lossy", "true", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.type", "VARLEN", tops, log);
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
