package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

import java.util.Set;

public abstract class IndexLookup {
    protected ShardQueryConfiguration config;
    protected ScannerFactory scannerFactory;
    protected boolean supportReference;
    
    protected boolean unfieldedLookup;
    protected Set<String> fields;
    
    protected IndexLookupMap indexLookupMap;
    
    public IndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean supportReference) {
        this.config = config;
        this.scannerFactory = scannerFactory;
        this.supportReference = supportReference;
    }
    
    public IndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        this(config, scannerFactory, false);
    }
    
    public void setup() {
        // intentionally left blank
    }
    
    public abstract IndexLookupMap lookup();
    
    public boolean isSupportReference() {
        return supportReference;
    }
}
