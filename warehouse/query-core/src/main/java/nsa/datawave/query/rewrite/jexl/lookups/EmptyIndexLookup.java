package nsa.datawave.query.rewrite.jexl.lookups;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.tables.ScannerFactory;

public class EmptyIndexLookup extends IndexLookup {
    @Override
    public IndexLookupMap lookup(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory, long lookupTimer) {
        return new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
    }
    
}
