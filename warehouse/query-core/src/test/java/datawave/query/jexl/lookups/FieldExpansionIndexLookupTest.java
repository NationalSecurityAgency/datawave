package datawave.query.jexl.lookups;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;

/**
 * Tests for the {@link FieldExpansionIndexLookup}
 */
public class FieldExpansionIndexLookupTest extends BaseIndexLookupTest {

    private static final Logger log = LoggerFactory.getLogger(FieldExpansionIndexLookupTest.class);

    @Test
    public void testValueDoesNotExpand() {
        // no data
        withQuery("_ANYFIELD_ == 'bar'");
        executeLookup();
        assertNoResults();
    }

    @Test
    public void testValueExpandsIntoSingleField() {
        write("bar", "FIELD_A");
        withQuery("_ANYFIELD_ == 'bar'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A"));
        assertResultValues("FIELD_A", Set.of("bar"));
    }

    @Test
    public void testValueExpandsIntoMultipleFields() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        withQuery("_ANYFIELD_ == 'bar'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B"));
        assertResultValues("FIELD_A", Set.of("bar"));
        assertResultValues("FIELD_B", Set.of("bar"));
    }

    @Test
    public void testValueExpandsIntoMultipleFieldsSkippingPreviouslyIndexedFields() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        write("bar", "FIELD_C");
        write("bar", "FIELD_X");
        withQuery("_ANYFIELD_ == 'bar'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
        assertResultValues("FIELD_A", Set.of("bar"));
        assertResultValues("FIELD_B", Set.of("bar"));
        assertResultValues("FIELD_C", Set.of("bar"));
    }

    @Test
    public void testExpansionHitsTimeoutThreshold() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B", EXCEPTEDVALUE);
        write("bar", "FIELD_C");
        withQuery("_ANYFIELD_ == 'bar'");
        executeLookup();
        assertResultFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
        assertResultValues("FIELD_A", Set.of("bar"));
        // field expansion iterator does not (currently) honor timeout exceptions
    }

    @Test
    public void testExpansionHitsValueThreshold_singleValueMultiField() {
        int valueExpansionThreshold = config.getMaxValueExpansionThreshold();
        try {
            write("bar", "FIELD_A");
            write("bar", "FIELD_B");
            write("bar", "FIELD_C");
            withQuery("_ANYFIELD_ == 'bar'");
            config.setMaxValueExpansionThreshold(1);
            executeLookup();
            assertResultFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
            assertResultValues("FIELD_A", Set.of("bar"));
            assertResultValues("FIELD_B", Set.of("bar"));
            assertResultValues("FIELD_C", Set.of("bar"));
        } finally {
            config.setMaxValueExpansionThreshold(valueExpansionThreshold);
        }
    }

    @Override
    protected void executeLookup() {
        try {
            Preconditions.checkNotNull(query, "query cannot be null");
            JexlNode node = parse(query);
            String field = JexlASTHelper.getIdentifier(node);
            assertEquals("_ANYFIELD_", field);

            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            String value = String.valueOf(literal);

            AsyncIndexLookup lookup = createLookup(value);
            executeLookup(lookup);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            fail("Lookup failed: " + e.getMessage());
        }
    }

    private AsyncIndexLookup createLookup(String value) {
        ScannerFactory scannerFactory = new ScannerFactory(client);
        return new FieldExpansionIndexLookup(config, scannerFactory, value, indexedFields, executor);
    }
}
