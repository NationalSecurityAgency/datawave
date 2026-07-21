package datawave.query.jexl.lookups;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.base.Preconditions;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.RefactoredRangeDescription;
import datawave.query.tables.ScannerFactory;

/**
 * Regression coverage for two fixed causes of {@link UnfieldedRegexIndexLookup} blocking query planning indefinitely: an unbounded default scan timeout, and a
 * scan loop that kept consuming rows after the field-count threshold was already exceeded.
 */
public class UnfieldedRegexIndexLookupHangTest extends BaseIndexLookupTest {

    /**
     * {@code maxAnyFieldScanTimeMillis} must default to a finite bound so a stalled scan is always eventually cancelled by the {@link ScanMonitor}.
     */
    @Test
    public void defaultAnyFieldScanTimeoutIsFinite() {
        assertEquals(TimeUnit.HOURS.toMillis(1), config.getMaxAnyFieldScanTimeMillis(),
                        "default max any field scan time should be a finite bound, not Long.MAX_VALUE");
    }

    /**
     * Once the field-count threshold is exceeded the scan's outcome is already decided, so the loop should stop instead of consuming the rest of the range.
     */
    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void scanDoesNotStopEarlyAfterKeyThresholdExceeded() throws Exception {
        int origThreshold = config.getMaxUnfieldedExpansionThreshold();
        long origTimeout = config.getMaxAnyFieldScanTimeMillis();
        try {
            config.setMaxUnfieldedExpansionThreshold(1);
            config.setMaxAnyFieldScanTimeMillis(Long.MAX_VALUE);

            int delayMillis = 75;
            int totalEntries = 30;
            addDelayIterator(delayMillis);

            // every entry uses a distinct field, so the 1-field threshold is exceeded almost immediately
            for (int i = 0; i < totalEntries; i++) {
                write("bar-" + i, "FIELD_" + i);
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");

            long start = System.currentTimeMillis();
            executeLookup();
            long elapsed = System.currentTimeMillis() - start;

            long allEntriesDuration = (long) totalEntries * delayMillis;
            assertTrue(elapsed < allEntriesDuration / 2,
                            "expected the scan to stop shortly after the field threshold was exceeded (well under " + (allEntriesDuration / 2)
                                            + "ms), but it took " + elapsed + "ms -- proportional to scanning all " + totalEntries
                                            + " remaining rows instead of stopping early");
        } finally {
            removeDelayIterator();
            config.setMaxUnfieldedExpansionThreshold(origThreshold);
            config.setMaxAnyFieldScanTimeMillis(origTimeout);
        }
    }

    @Override
    protected void executeLookup() throws Exception {
        Preconditions.checkNotNull(query, "query cannot be null");
        JexlNode node = parse(query);
        String field = JexlASTHelper.getIdentifier(node);
        assertEquals(Constants.ANY_FIELD, field);

        Object literal = JexlASTHelper.getLiteralValueSafely(node);
        String value = String.valueOf(literal);

        RefactoredRangeDescription desc = ShardIndexQueryTableStaticMethods.getRegexRange(field, value, false, metadataHelper, config);
        Range range = desc.range;
        boolean reverse = desc.isForReverseIndex;

        ScannerFactory scannerFactory = new ScannerFactory(client);
        AsyncIndexLookup lookup = new UnfieldedRegexIndexLookup(config, scannerFactory, executor, value, range, reverse, null);
        lookup.setScanMonitor(monitor);
        executeLookup(lookup);
    }
}
