package datawave.query.jexl.lookups;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.RefactoredRangeDescription;
import datawave.query.tables.ScannerFactory;
import datawave.util.TableName;

/**
 * Some basic tests for the {@link UnfieldedRegexIndexLookup}
 */
public class UnfieldedRegexIndexLookupTest extends BaseIndexLookupTest {

    private static final Logger log = LoggerFactory.getLogger(UnfieldedRegexIndexLookupTest.class);

    @Test
    public void testExpansionZeroHits() {
        // no data
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testExpansionOneHit() {
        write("bar", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar"));
    }

    @Test
    public void testExpandsIntoSingleFieldWithMultipleValues() {
        write("bar", "FIELD");
        write("baz", "FIELD");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("bar", "baz"));
    }

    @Test
    public void testExpandsIntoMultipleFieldsWithSingleValues() {
        write("bar", "FIELD_A");
        write("barstool", "FIELD_B");
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("bar"));
        assertResultValues("FIELD_B", Set.of("barstool"));
    }

    @Test
    public void testExpandsIntoMultipleFieldsWithMultipleValues() {
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
    public void testSimulatedTimeout() {
        write("bar", "FIELD_A");
        write("baz", "FIELD_A");
        write("baz-kaboom", "FIELD_A", EXCEPTEDVALUE);
        withQuery("_ANYFIELD_ =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar", "baz"));
        assertTimeoutExceeded();
    }

    @Test
    public void testReverseExpansionZeroHits() {
        // no data
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testReverseExpansionOneHit() {
        writeReverse("tim", "FIELD");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("tim"));
    }

    @Test
    public void testReverseExpandsIntoSingleFieldWithMultipleValues() {
        writeReverse("tim", "FIELD");
        writeReverse("tom", "FIELD");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD"));
        assertResultValues("FIELD", Set.of("tim", "tom"));
    }

    @Test
    public void testReverseExpandsIntoMultipleFieldsWithSingleValues() {
        writeReverse("tim", "FIELD_A");
        writeReverse("tom", "FIELD_B");
        withQuery("_ANYFIELD_ =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("tim"));
        assertResultValues("FIELD_B", Set.of("tom"));
    }

    @Test
    public void testReverseExpandsIntoMultipleFieldsWithMultipleValues() {
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

    /**
     * Build an index lookup from the query and store the results
     */
    @Override
    protected void executeLookup() {
        try {
            Preconditions.checkNotNull(query, "query cannot be null");
            JexlNode node = parse(query);
            String field = JexlASTHelper.getIdentifier(node);
            assertEquals(Constants.ANY_FIELD, field);

            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            String value = String.valueOf(literal);

            RefactoredRangeDescription desc = ShardIndexQueryTableStaticMethods.getRegexRange(field, value, false, metadataHelper, config);
            Range range = desc.range;
            boolean reverse = desc.isForReverseIndex;

            AsyncIndexLookup lookup = createLookup(value, range, reverse, null);
            executeLookup(lookup);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            fail("Lookup failed: " + e.getMessage());
        }
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
        return new UnfieldedRegexIndexLookup(config, scannerFactory, executor, regex, range, reverse, fields);
    }
}
