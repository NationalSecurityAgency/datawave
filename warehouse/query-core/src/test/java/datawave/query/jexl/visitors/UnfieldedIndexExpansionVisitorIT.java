package datawave.query.jexl.visitors;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.model.QueryModel;

/**
 * Collection of tests for the {@link UnfieldedIndexExpansionVisitor}
 */
public class UnfieldedIndexExpansionVisitorIT extends BaseIndexExpansionTest {

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "_ANYFIELD_");

    @BeforeEach
    public void beforeEach() throws Exception {
        super.beforeEach();
        config.setIndexedFields(indexedFields);
        helper.setIndexedFields(indexedFields);

        config.setReverseIndexedFields(indexedFields);
        helper.setReverseIndexFields(indexedFields);

        Multimap<String,String> compositeToFieldMap = HashMultimap.create();
        compositeToFieldMap.put("COMPOSITE", "X");
        compositeToFieldMap.put("COMPOSITE", "Y");
        config.setCompositeToFieldMap(compositeToFieldMap);
        helper.setCompositeToFieldMap(compositeToFieldMap);

        QueryModel queryModel = new QueryModel();
        queryModel.addTermToModel("FIELD_A", "FIELD_A");
        queryModel.addTermToModel("FOO", "FIELD_A");
        config.setModelName("model-name");
        config.setQueryModel(queryModel);
        helper.setQueryModel("model-name", queryModel);
    }

    @Test
    public void testNoData() throws Exception {
        // no data
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "_NOFIELD_ =~ 'ba.*'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoSingleFieldSingleValue() throws Exception {
        write("bar", "FIELD_A");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoSingleFieldMultipleValue() throws Exception {
        write("bar", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoMultipleFieldsWithSingleValue() throws Exception {
        write("bar", "FIELD_A");
        write("baz", "FIELD_B");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_B == 'baz'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoMultipleFieldsWithMultipleValues() throws Exception {
        write("tim", "FIELD_A");
        write("tom", "FIELD_A");
        write("tim", "FIELD_B");
        write("tam", "FIELD_B");
        String query = "_ANYFIELD_ =~ 't.*'";
        String expected = "FIELD_A == 'tim' || FIELD_A == 'tom' || FIELD_B == 'tim' || FIELD_B == 'tam'";
        driveExpansion(query, expected);
    }

    @Test
    public void testLimitFieldExpansionToQueryModelForwardMapping() throws Exception {
        write("tim", "FIELD_A");
        write("tom", "FIELD_A");
        write("tim", "FIELD_B");
        write("tam", "FIELD_B");
        String query = "_ANYFIELD_ =~ 't.*'";
        String expected = "FIELD_A == 'tim' || FIELD_A == 'tom'";
        config.setLimitTermExpansionToModel(true);
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoCompositeFieldsPrevented() throws Exception {
        write("bar", "FIELD_A");
        write("bar", "COMPOSITE");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testNegatedRegexExpandsToSingleValue() throws Exception {
        write("bar", "FIELD_A");
        String query = "!(_ANYFIELD_ =~ 'ba.*')";
        String expected = "!(_ANYFIELD_ =~ 'ba.*' || FIELD_A == 'bar')";
        // original node retained...for reasons
        driveExpansion(query, expected);
    }

    @Test
    public void testNegatedRegexExpansionPrevented() throws Exception {
        write("bar", "FIELD_A");
        String query = "!(_ANYFIELD_ =~ 'ba.*')";
        config.setExpandUnfieldedNegations(false);
        driveExpansion(query, query);
    }

    @Test
    public void testLiteralExpandsToSingleField() throws Exception {
        write("bar", "FIELD_A");
        String query = "_ANYFIELD_ == 'bar'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testLiteralExpandsToMultipleFields() throws Exception {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        String query = "_ANYFIELD_ == 'bar'";
        String expected = "FIELD_A == 'bar' || FIELD_B == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testMixOfRegexesAndLiteralsExpanded() throws Exception {
        write("bar", "FIELD_A");
        write("baz", "FIELD_B");
        write("value", "FIELD_A");
        write("value", "FIELD_B");
        String query = "_ANYFIELD_ =~ 'ba.*' && _ANYFIELD_ == 'value'";
        String expected = "(FIELD_A == 'bar' || FIELD_B == 'baz') && (FIELD_A == 'value' || FIELD_B == 'value')";
        driveExpansion(query, expected);
    }

    @Test
    public void testRegexExceedsValueExpansionThreshold() throws Exception {
        write("bar", "FIELD_A");
        write("bat", "FIELD_B");
        write("baz", "FIELD_C");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_B == 'bat' || FIELD_C == 'baz'";
        config.setMaxValueExpansionThreshold(1);
        driveExpansion(query, expected);
    }

    @Test
    public void testRegexExceedsKeyExpansionThreshold() {
        write("bar", "FIELD_A");
        write("bat", "FIELD_B");
        write("baz", "FIELD_C");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        config.setMaxUnfieldedExpansionThreshold(1);
        assertThrows(DatawaveFatalQueryException.class, () -> driveExpansion(query, null));
    }

    @Test
    public void testRegexExceedsTimeThresholds() {
        write("bar", "FIELD_A");
        write("bat", "FIELD_B", EXCEPTEDVALUE); // should simulate a timeout
        write("baz", "FIELD_C");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        assertThrows(DatawaveFatalQueryException.class, () -> driveExpansion(query, null));
    }

    @Test
    public void testEqualityExceedsValueThreshold() throws Exception {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        write("bar", "FIELD_C");
        String query = "_ANYFIELD_ == 'bar'";
        String expected = "FIELD_B == 'bar' || FIELD_A == 'bar' || FIELD_C == 'bar'";
        // NOTE: value expansion threshold is supposed to be enforced as a limit for the original node, this seems
        // like an improper application of the threshold.
        config.setMaxValueExpansionThreshold(1);
        driveExpansion(query, expected);
    }

    @Test
    public void testEqualityExceedsKeyThreshold() {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        write("bar", "FIELD_C");
        String query = "_ANYFIELD_ == 'bar'";
        config.setMaxUnfieldedExpansionThreshold(1);
        assertThrows(DatawaveFatalQueryException.class, () -> driveExpansion(query, null));
    }

    @Test
    public void testEqualityExceedsTimeoutThreshold() throws Exception {
        write("bar", "FIELD_A");
        write("bar", "FIELD_B", EXCEPTEDVALUE);
        write("bar", "FIELD_C");
        String query = "_ANYFIELD_ == 'bar'";
        String expected = "FIELD_A == 'bar' || FIELD_B == 'bar' || FIELD_C == 'bar'";
        // timeout is ignored
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeNoMatches() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "_NOFIELD_ =~ 'ba.*'";
        config.setDatatypeFilter(Set.of("datatype-z"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypePartialMatch() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        config.setDatatypeFilter(Set.of("datatype-a"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeFullMatch() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        config.setDatatypeFilter(Set.of("datatype-a", "datatype-b"));
        driveExpansion(query, expected);
    }

    @Override
    protected JexlNode expand(ASTJexlScript script) throws Exception {
        return UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, helper, script);
    }
}
