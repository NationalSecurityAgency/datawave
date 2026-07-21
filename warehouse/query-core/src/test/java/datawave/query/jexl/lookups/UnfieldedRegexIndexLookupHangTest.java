package datawave.query.jexl.lookups;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

import com.google.common.base.Preconditions;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.RefactoredRangeDescription;
import datawave.query.tables.ScannerFactory;

/**
 * Demonstrates two compounding causes that let an {@link UnfieldedRegexIndexLookup} block the calling (query-planning) thread indefinitely:
 * <ol>
 * <li>{@code ShardQueryConfiguration#maxAnyFieldScanTimeMillis} defaults to {@code Long.MAX_VALUE}. The {@link ScanMonitor} only cancels a future once
 * {@code currentMillis - startMillis >= timeoutMillis}, so with the default value that condition is never true in practice -- registering a task with this
 * "timeout" is equivalent to registering no timeout at all. {@link AsyncIndexLookup#await()}/{@link BaseRegexIndexLookup#await()} then call
 * {@code future.get()} with no timeout of their own, relying entirely on the (never-firing) monitor.</li>
 * <li>{@link UnfieldedRegexIndexLookup}'s scan loop never checks {@link IndexLookupMap#isKeyThresholdExceeded()} (nor the boolean return of
 * {@link IndexLookupMap#put}). Once the max-distinct-fields threshold is exceeded, every subsequent {@code put} silently becomes a no-op, but the loop keeps
 * consuming the scanner instead of breaking out, so the scan runs to the end of its (potentially very large, since it is unfielded and spans many column
 * families) range regardless of the fact that the outcome is already decided.</li>
 * </ol>
 * Combined, a broad/poorly-anchored unfielded regex over a large index can run far longer than necessary, and -- since cause (1) removes the only safety net --
 * can block query planning indefinitely.
 */
public class UnfieldedRegexIndexLookupHangTest extends BaseIndexLookupTest {

    /**
     * Demonstrates cause (1) in isolation: with {@code maxAnyFieldScanTimeMillis} left at its default (unbounded), a stalled/slow scan (simulated here with a
     * per-entry delay, standing in for e.g. a slow or unresponsive tablet server) is not bounded by anything -- the lookup blocks for as long as the underlying
     * scan takes.
     * <p>
     * The outer {@link Timeout} exists only so that if this test fails by actually hanging, the build fails fast instead of hanging forever; the
     * {@code assertTimeoutPreemptively} call is the actual assertion under test and is expected to fail on current (buggy) code.
     */
    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void unboundedDefaultTimeoutProvidesNoProtection() throws Exception {
        assertEquals(Long.MAX_VALUE, config.getMaxAnyFieldScanTimeMillis(), "sanity check: default max any field scan time is unbounded");

        addDelayIterator(3000);
        try {
            write("bar", "FIELD_A");
            withQuery("_ANYFIELD_ =~ 'ba.*'");

            assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) this::executeLookup,
                            "lookup() should not block indefinitely, but with the default (unbounded) maxAnyFieldScanTimeMillis "
                                            + "there is no mechanism -- neither the ScanMonitor nor future.get() -- that bounds a stalled scan");
        } finally {
            removeDelayIterator();
        }
    }

    /**
     * Demonstrates cause (2) in isolation: once the max-distinct-fields threshold is exceeded, the outcome of the scan is already decided (the term expansion
     * has failed), yet the scan loop keeps consuming every remaining row instead of stopping, so elapsed time scales with the total number of rows in the range
     * rather than with how quickly the threshold was hit.
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
