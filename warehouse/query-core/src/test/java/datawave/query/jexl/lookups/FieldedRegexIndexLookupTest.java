package datawave.query.jexl.lookups;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.RefactoredRangeDescription;
import datawave.query.tables.ScannerFactory;

public class FieldedRegexIndexLookupTest extends BaseIndexLookupTest {

    private static final Logger log = LoggerFactory.getLogger(FieldedRegexIndexLookupTest.class);

    @Test
    public void testExpansionWithNodata() {
        // no data
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testExpandsIntoSingleValue() {
        write("bar", "FIELD_A");
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar"));
    }

    @Test
    public void testExpandsIntoMultipleValues() {
        write("bar", "FIELD_A");
        write("baz", "FIELD_A");
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar", "baz"));
    }

    @Test
    public void testExpandsIntoMultipleValuesSkipsDifferentField() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        write("baz", "FIELD_A");
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar", "baz"));
    }

    @Test
    public void testExpansionSkipsPreviouslyIndexedFields() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_X");
        write("baz", "FIELD_A");
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar", "baz"));
    }

    @Test
    public void testExpansionTimeoutFailure() {
        write("bar", "FIELD_A");
        write("baz", "FIELD_A", EXCEPTEDVALUE);
        withQuery("FIELD_A =~ 'ba.*'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertThrows(ExceededThresholdException.class, () -> assertResultValues("FIELD_A", Set.of("bar")));
        assertTimeoutExceeded();
    }

    @Test
    public void testReverseExpansionNoData() {
        // no data
        withQuery("FIELD_A =~ '.*ab'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testReverseExpansionSingleValue() {
        writeReverse("tim", "FIELD_A");
        withQuery("FIELD_A =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("tim"));
    }

    @Test
    public void testReverseExpansionMultipleValues() {
        writeReverse("tim", "FIELD_A");
        writeReverse("tam", "FIELD_A");
        withQuery("FIELD_A =~ '.*m'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("tim", "tam"));
    }

    @Override
    protected void executeLookup() {
        try {
            Preconditions.checkNotNull(query, "query cannot be null");
            JexlNode node = parse(query);
            String field = JexlASTHelper.getIdentifier(node);

            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            String value = String.valueOf(literal);

            RefactoredRangeDescription desc = ShardIndexQueryTableStaticMethods.getRegexRange(field, value, false, metadataHelper, config);
            Range range = desc.range;
            boolean reverse = desc.isForReverseIndex;

            AsyncIndexLookup lookup = createLookup(field, value, range, reverse);
            executeLookup(lookup);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            fail("Lookup failed: " + e.getMessage());
        }
    }

    private AsyncIndexLookup createLookup(String field, String value, Range range, boolean reverse) {
        ScannerFactory scannerFactory = new ScannerFactory(client);
        return new FieldedRegexIndexLookup(config, scannerFactory, executor, field, value, range, reverse);
    }
}
