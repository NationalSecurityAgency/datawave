package datawave.query.jexl.lookups;

import static datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.EXPANSION_HINT_KEY;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

/**
 * Holds common variables to reduce duplicate code in {@link FieldedRegexIndexLookup} and {@link UnfieldedRegexIndexLookup}.
 */
public abstract class BaseRegexIndexLookup extends AsyncIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(BaseRegexIndexLookup.class);

    protected final String pattern;
    protected final Range range;
    protected final boolean reverse;

    protected Future<?> future;

    // used when scanning the shard reverse index
    private final StringBuilder sb = new StringBuilder();

    public BaseRegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean unfieldedLookup, ExecutorService execService,
                    String pattern, Range range, boolean reverse) {
        super(config, scannerFactory, unfieldedLookup, execService);
        this.pattern = pattern;
        this.range = range;
        this.reverse = reverse;
    }

    protected String getTableName() {
        if (reverse) {
            // there is only one supported reverse index
            return config.getReverseIndexTableName();
        }
        return super.getTableName();
    }

    protected String getHintKey(String tableName) {
        return config.getTableHints().containsKey(EXPANSION_HINT_KEY) ? EXPANSION_HINT_KEY : tableName;
    }

    /**
     * Extending classes decide how to build a regex iterator depending on if the regex is fielded or not
     *
     * @return an {@link IteratorSetting}
     */
    protected abstract IteratorSetting createRegexIterator();

    /**
     * Waits for the future to complete before returning the index lookup map
     */
    protected void await() {
        try {
            if (future != null) {
                future.get();
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    /**
     * Extending classes deal with exceptions differently
     *
     * @param e
     *            the exception
     */
    protected abstract void handleException(Exception e);

    /**
     * Reverses the value coming off the shard reverse index
     *
     * @param value
     *            the value
     * @return the reversed value
     */
    protected String reverse(String value) {
        sb.setLength(0);
        sb.append(value);
        sb.reverse();
        return sb.toString();
    }
}
