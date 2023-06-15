package datawave.ingest.table.config;

import datawave.ingest.mapreduce.handler.error.ErrorShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Class to create the Metadata table for the sharded error tables. Extends the MetadataTableConfigHelper since all we want to do differently is change the name
 * of the Metadata table that will be used.
 *
 *
 */
public class ErrorMetadataTableConfigHelper extends MetadataTableConfigHelper {
    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        super.configure(tops);
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            configureToDropBadData(tops, scope.name());
        }
    }

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {

        this.log = log;
        this.conf = config;
        this.tableName = conf.get(ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ShardedDataTypeHandler.METADATA_TABLE_NAME, null);

        if (this.tableName == null || !this.tableName.equals(tableName)) {
            throw new IllegalArgumentException("No Such Table: " + tableName);
        }
    }

    // MetricsFileProtoIngestHelper creates nonsense field names, each containing a '.'
    private void configureToDropBadData(TableOperations tops, String scopeName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "dropBadData");
        setPropertyIfNecessary(this.tableName, stem, "30,org.apache.accumulo.core.iterators.user.RegExFilter", tops, log);
        setPropertyIfNecessary(this.tableName, stem + ".opt.negate", "true", tops, log);
        setPropertyIfNecessary(this.tableName, stem + ".opt.rowRegex", ".*\\..*", tops, log);
        setPropertyIfNecessary(this.tableName, stem + ".opt.encoding", "UTF-8", tops, log);
    }
}
