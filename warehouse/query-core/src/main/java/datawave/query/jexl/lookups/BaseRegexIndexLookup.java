package datawave.query.jexl.lookups;

import static datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.EXPANSION_HINT_KEY;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.TimeoutIterator;
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

    protected final CountDownLatch latch;

    // used when scanning the shard reverse index
    private final StringBuilder sb = new StringBuilder();

    public BaseRegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean unfieldedLookup, ExecutorService execService,
                    String pattern, Range range, boolean reverse) {
        super(config, scannerFactory, unfieldedLookup, execService);
        this.pattern = pattern;
        this.range = range;
        this.reverse = reverse;
        this.latch = new CountDownLatch(1);
    }

    protected String getTableName() {
        return reverse ? config.getReverseIndexTableName() : config.getIndexTableName();
    }

    protected String getHintKey(String tableName) {
        return config.getTableHints().containsKey(EXPANSION_HINT_KEY) ? EXPANSION_HINT_KEY : tableName;
    }

    protected IteratorSetting createTimeoutIterator() {
        long maxTime = (long) (config.getMaxIndexScanTimeMillis() * 1.25);
        IteratorSetting iterator = new IteratorSetting(1, TimeoutIterator.class);
        iterator.addOption(TimeoutIterator.MAX_SESSION_TIME, Long.valueOf(maxTime).toString());
        return iterator;
    }

    /**
     * Extending classes decide how to build a regex iterator depending on if the regex is fielded or not
     *
     * @return an {@link IteratorSetting}
     */
    protected abstract IteratorSetting createRegexIterator();

    protected IteratorSetting createTimeoutExceptionIterator() {
        return new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
    }

    /**
     * Waits for the future to complete before returning the index lookup map
     */
    protected void await() {
        synchronized (latch) {
            if (latch.getCount() == 1) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

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
