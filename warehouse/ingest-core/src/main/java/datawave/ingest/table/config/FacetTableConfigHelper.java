package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.facet.FacetHandler;

public class FacetTableConfigHelper extends AbstractTableConfigHelper {

    protected Logger log;

    public enum FacetTableType {
        FACET, HASH, META
    }

    protected Configuration conf;
    protected String tableName;
    protected String facetTableName;
    protected String facetMetadataTableName;
    protected String facetHashTableName;
    protected FacetTableType tableType;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        this.log = log;
        this.conf = config;

        facetTableName = conf.get(FacetHandler.FACET_TABLE_NAME, null);
        facetMetadataTableName = conf.get(FacetHandler.FACET_METADATA_TABLE_NAME, null);
        facetHashTableName = conf.get(FacetHandler.FACET_HASH_TABLE_NAME, null);

        if (facetTableName == null && facetMetadataTableName == null && facetHashTableName == null) {
            throw new IllegalArgumentException("No Facet Table names are defined");
        }

        // TODO: generic balancer, markings, bloom filters, locality groups.

        if (tableName.equals(facetTableName)) {
            this.tableType = FacetTableType.FACET;
        } else if (tableName.equals(facetMetadataTableName)) {
            this.tableType = FacetTableType.META;
        } else if (tableName.equals(facetHashTableName)) {
            this.tableType = FacetTableType.HASH;
        } else {
            throw new IllegalArgumentException("Invalid Facet Table Definition Fot: " + tableName);
        }
        this.tableName = tableName;
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        switch (this.tableType) {
            case FACET:
                configureFacetTable(tops);
                break;
            case META:
                configureFacetMetadataTable(tops);
                break;
            case HASH:
                configureFacedHashTable(tops);
                break;
            default:
                // Technically, this is dead code. If 'Configure' is called prior to 'Setup'
                // tableType is null and throws a NullPointerException in the switch statement.
                // If 'Setup' successfully runs to completion then tableType is assigned one
                // of the three other values.
                throw new TableNotFoundException(null, tableName, "Table is not a Facet Type Table");
        }
    }

    protected void configureFacetTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Add the facet cardinality aggregator
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            setPropertyIfNecessary(tableName, stem, "19,datawave.iterators.TotalAggregatingIterator", tops, log);
            stem += ".opt.";
            setPropertyIfNecessary(tableName, stem + "*", "datawave.ingest.table.aggregator.CardinalityAggregator", tops, log);
        }
    }

    protected void configureFacetMetadataTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // TODO:
    }

    protected void configureFacedHashTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // TODO:
    }
}
