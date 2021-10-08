package datawave.query.jexl.lookups;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

public abstract class BaseRegexIndexLookup extends AsyncIndexLookup {
    public static final Set<String> DISALLOWED_PATTERNS = Sets.newHashSet(".*", ".*?");
    
    public BaseRegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean unfieldedLookup, ExecutorService execService) {
        super(config, scannerFactory, unfieldedLookup, execService);
    }
    
    public static boolean isAcceptedPattern(String pattern) {
        return !DISALLOWED_PATTERNS.contains(pattern);
    }
}
