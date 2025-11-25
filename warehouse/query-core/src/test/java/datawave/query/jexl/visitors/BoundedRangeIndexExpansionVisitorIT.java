package datawave.query.jexl.visitors;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Collection of tests for the {@link BoundedRangeIndexExpansionVisitor}
 */
public class BoundedRangeIndexExpansionVisitorIT extends BaseIndexExpansionTest {

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C");

    @BeforeEach
    public void beforeEach() throws Exception {
        super.beforeEach();
        config.setIndexedFields(indexedFields);
        helper.setIndexedFields(indexedFields);
    }

    @Test
    public void testExpansionNoData() throws Exception {
        // no data
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        driveExpansion(query, "false");
    }

    @Test
    public void testExpansionFieldNotIndexed() throws Exception {
        // no data
        String query = "((_Bounded_ = true) && (FIELD_X >= 'a' && FIELD_X <= 'c'))";
        driveExpansion(query, query);
    }

    @Test
    public void testExpansionIntoSingleValue() throws Exception {
        write("b", "FIELD_A");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "(FIELD_A == 'b')";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoMultipleValues() throws Exception {
        write("a", "FIELD_A");
        write("b", "FIELD_A");
        write("c", "FIELD_A");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "(FIELD_A == 'a' || FIELD_A == 'b' || FIELD_A == 'c')";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionSkipsCompositeField() {
        // really, verify that composite fields are retained
    }

    @Test
    public void testSimulatedTimeout() throws Exception {
        write("a", "FIELD_A");
        write("b", "FIELD_A", EXCEPTEDVALUE);
        write("c", "FIELD_A");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c')))";
        driveExpansion(query, expected);
    }

    @Test
    public void testNegatedRangeIsStillExpanded() throws Exception {
        write("b", "FIELD_A");
        String query = "!((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "!(FIELD_A == 'b')";
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeFilterNoMatches() throws Exception {
        write("a", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("b", "FIELD_A", DEFAULT_DATE, "datatype-b");
        write("c", "FIELD_A", DEFAULT_DATE, "datatype-c");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "(false)";
        config.setDatatypeFilter(Set.of("datatype-z"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeFilterPartialMatch() throws Exception {
        write("a", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("b", "FIELD_A", DEFAULT_DATE, "datatype-b");
        write("c", "FIELD_A", DEFAULT_DATE, "datatype-c");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "(FIELD_A == 'a' || FIELD_A == 'c')";
        config.setDatatypeFilter(Set.of("datatype-a", "datatype-c"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeFilterFullMatch() throws Exception {
        write("a", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("b", "FIELD_A", DEFAULT_DATE, "datatype-b");
        write("c", "FIELD_A", DEFAULT_DATE, "datatype-c");
        String query = "((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'c'))";
        String expected = "(FIELD_A == 'a' || FIELD_A == 'b' || FIELD_A == 'c')";
        config.setDatatypeFilter(Set.of("datatype-a", "datatype-b", "datatype-c"));
        driveExpansion(query, expected);
    }

    @Override
    protected JexlNode expand(ASTJexlScript script) throws Exception {
        return BoundedRangeIndexExpansionVisitor.expandBoundedRanges(config, scannerFactory, helper, script);
    }
}
