package datawave.query.jexl.lookups;

import java.util.Set;

import datawave.query.config.ImmutableShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

/**
 * Abstract class which provides a framework for index lookups
 */
public abstract class IndexLookup {
    protected ImmutableShardQueryConfiguration config;
    protected ScannerFactory scannerFactory;

    protected Set<String> fields;

    protected IndexLookupMap indexLookupMap;

    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, may be null
     */
    public IndexLookup(ImmutableShardQueryConfiguration config, ScannerFactory scannerFactory) {
        this.config = config;
        this.scannerFactory = scannerFactory;
    }

    public abstract IndexLookupMap lookup();
}
