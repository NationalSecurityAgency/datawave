package datawave.query.jexl.lookups;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

import datawave.core.iterators.IteratorTimeoutException;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.RefactoredRangeDescription;
import datawave.query.tables.ScannerFactory;
import datawave.table.constants.TableName;

/**
 * Some basic tests for the {@link UnfieldedRegexIndexLookup}
 */
public class UnfieldedRegexIndexLookupTest extends BaseIndexLookupTest {

    private AsyncIndexLookup lookup;

    @Test
    public void testExpansionZeroHits() throws Exception {
        // no data
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testExpansionOneHit() throws Exception {
        write("bar", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar"));
    }

    @Test
    public void testExpandsIntoSingleFieldWithMultipleValues() throws Exception {
        write("bar", "FIELD");
        write("baz", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar", "baz"));
    }

    @Test
    public void testExpandsIntoMultipleFieldsWithSingleValues() throws Exception {
        write("bar", "FIELD_A");
        write("barstool", "FIELD_B");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("bar"));
        assertResultValues("FIELD_B", Set.of("barstool"));
    }

    @Test
    public void testExpandsIntoMultipleFieldsWithMultipleValues() throws Exception {
        write("bar", "FIELD_A");
        write("barstool", "FIELD_A");
        write("baz", "FIELD_B");
        write("bazaar", "FIELD_B");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("bar", "barstool"));
        assertResultValues("FIELD_B", Set.of("baz", "bazaar"));
    }

    @Test
    public void testExpansionTimeoutOnInitialSeek() throws Exception {
        long origIndexScanTime = config.getMaxAnyFieldScanTimeMillis();
        try {
            addDelayIterator(10);
            config.setMaxAnyFieldScanTimeMillis(5);

            // ensure the test always hits the timeout
            for (int i = 0; i < 15; i++) {
                write("bar-" + i, "FIELD_A");
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");
            executeLookup();
            assertTimeoutExceeded();
            assertResultFields(Collections.emptySet());
        } finally {
            removeDelayIterator();
            config.setMaxAnyFieldScanTimeMillis(origIndexScanTime);
        }
    }

    @Test
    public void testExpansionTimeoutOnNext() throws Exception {
        long origIndexScanTime = config.getMaxAnyFieldScanTimeMillis();
        try {
            addDelayIterator(1);
            config.setMaxAnyFieldScanTimeMillis(5);

            // ensure the test always hits the timeout
            for (int i = 0; i < 15; i++) {
                write("bar-" + i, "FIELD_A");
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");
            executeLookup();
            assertTimeoutExceeded();
            assertResultFields(Collections.emptySet());
        } finally {
            removeDelayIterator();
            config.setMaxAnyFieldScanTimeMillis(origIndexScanTime);
        }
    }

    @Test
    public void testInitialSeekIteratorTimeoutException() throws Exception {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "Timeout for test", "seek");

            for (int i = 0; i < 15; i++) {
                write("bar-" + i, "FIELD_A");
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");
            executeLookup();
            // a partial expansion will occur
            assertTimeoutExceeded();
            assertNoResults();
        } finally {
            removeIOExceptionIterator();
        }
    }

    @Test
    public void testInitialSeekIteratorInterruptedException() throws Exception {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IterationInterrupted for test", "seek");

            for (int i = 0; i < 15; i++) {
                write("bar-" + i, "FIELD_A");
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");
            executeLookup();
            assertResultFields(Collections.emptySet());
            assertExceptionSeen();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testInitialSeekNullPointerException() throws Exception {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "seek");

            // ensure the test always hits the timeout
            for (int i = 0; i < 15; i++) {
                write("bar-" + i, "FIELD_A");
            }

            withQuery("_ANYFIELD_ =~ 'ba.*'");
            executeLookup();
            assertResultFields(Collections.emptySet());
            assertExceptionSeen();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testReverseExpansionZeroHits() throws Exception {
        // no data
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testReverseExpansionOneHit() throws Exception {
        writeReverse("tim", "FIELD");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("tim"));
    }

    @Test
    public void testReverseExpandsIntoSingleFieldWithMultipleValues() throws Exception {
        writeReverse("tim", "FIELD");
        writeReverse("tom", "FIELD");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("tim", "tom"));
    }

    @Test
    public void testReverseExpandsIntoMultipleFieldsWithSingleValues() throws Exception {
        writeReverse("tim", "FIELD_A");
        writeReverse("tom", "FIELD_B");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("tim"));
        assertResultValues("FIELD_B", Set.of("tom"));
    }

    @Test
    public void testReverseExpandsIntoMultipleFieldsWithMultipleValues() throws Exception {
        writeReverse("tim", "FIELD_A");
        writeReverse("tom", "FIELD_A");
        writeReverse("tim", "FIELD_B");
        writeReverse("tam", "FIELD_B");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("tim", "tom"));
        assertResultValues("FIELD_B", Set.of("tim", "tam"));
    }

    @Test
    public void testExecutionHints_expansionPoolSelectedOverIndexTable() throws Exception {
        write("bar", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");

        Map<String,String> expansionHints = new HashMap<>();
        expansionHints.put("scan_type", "expansion-pool-a");
        expansionHints.put("priority", "2");

        Map<String,String> indexHints = new HashMap<>();
        indexHints.put("scan_type", "index-a");
        indexHints.put("priority", "1");

        Map<String,Map<String,String>> tableHints = new HashMap<>();
        tableHints.put("expansion", expansionHints);
        tableHints.put("shardIndex", indexHints);
        config.setTableHints(tableHints);

        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar"));

        assertNotNull(lookup.builder);
        assertEquals(expansionHints, lookup.builder.getExecutionHints());
    }

    @Test
    public void testExecutionHints_indexTableNameSelectedWhenNoExpansionPoolExists() throws Exception {
        write("bar", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");

        Map<String,String> indexHints = new HashMap<>();
        indexHints.put("scan_type", "index-a");
        indexHints.put("priority", "1");

        Map<String,Map<String,String>> tableHints = new HashMap<>();
        tableHints.put("shardIndex", indexHints);
        config.setTableHints(tableHints);

        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar"));

        assertNotNull(lookup.builder);
        assertEquals(indexHints, lookup.builder.getExecutionHints());
    }

    @Test
    public void testConsistencyLevel() throws Exception {
        write("bar", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");

        Map<String,ScannerBase.ConsistencyLevel> consistencyLevels = new HashMap<>();
        consistencyLevels.put("shardIndex", ConsistencyLevel.EVENTUAL);
        config.setTableConsistencyLevels(consistencyLevels);

        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar"));

        assertNotNull(lookup.builder);
        assertEquals(ConsistencyLevel.EVENTUAL, lookup.builder.getConsistencyLevel());
    }

    /**
     * Build an index lookup from the query and store the results
     */
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

        lookup = createLookup(value, range, reverse, null);
        executeLookup(lookup);
    }

    /**
     * Create an {@link UnfieldedRegexIndexLookup}
     *
     * @param regex
     *            the regex
     * @param range
     *            the range
     * @param reverse
     *            flag denoting use of the {@link TableName#SHARD_RINDEX}
     * @param fields
     *            an optional set of fields used to restrict the scan
     * @return an UnfieldedRegexIndexLookup
     */
    private AsyncIndexLookup createLookup(String regex, Range range, boolean reverse, Set<String> fields) {
        ScannerFactory scannerFactory = new ScannerFactory(client);
        AsyncIndexLookup lookup = new UnfieldedRegexIndexLookup(config, scannerFactory, executor, regex, range, reverse, fields);
        lookup.setScanMonitor(monitor);
        return lookup;
    }
}
