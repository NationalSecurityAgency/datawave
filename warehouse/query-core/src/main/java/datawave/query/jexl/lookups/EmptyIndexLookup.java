package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

import java.util.concurrent.ExecutorService;

public class EmptyIndexLookup extends IndexLookup {
    @Override
    public void lookupAsync(ShardQueryConfiguration config, ScannerFactory scannerFactory, long timer, ExecutorService execService) {
        this.config = config;
        this.scannerFactory = scannerFactory;
        this.timer = timer;
        this.execService = execService;
        
        lookupStartTimeMillis = System.currentTimeMillis();
    }
    
    @Override
    public IndexLookupMap lookupWait() {
        return new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
    }
}
