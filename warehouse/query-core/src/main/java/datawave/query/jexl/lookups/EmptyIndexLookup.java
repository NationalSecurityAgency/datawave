package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

public class EmptyIndexLookup extends IndexLookup {
    @Override
    public IndexLookupMap lookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, long lookupTimer) {
        return new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
    }
    
}
