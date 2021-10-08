package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

public class EmptyIndexLookup extends IndexLookup {
    
    public EmptyIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        super(config, scannerFactory);
    }
    
    @Override
    public IndexLookupMap lookup() {
        return new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
    }
}
