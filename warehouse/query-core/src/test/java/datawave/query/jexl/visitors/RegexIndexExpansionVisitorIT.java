package datawave.query.jexl.visitors;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.model.QueryModel;

/**
 * Collection of tests for the {@link RegexIndexExpansionVisitor}
 */
public class RegexIndexExpansionVisitorIT extends BaseIndexExpansionTest {

    private final Set<String> indexedFields = Set.of("FIELD_A", "COMPOSITE");

    @BeforeEach
    public void beforeEach() throws Exception {
        super.beforeEach();
        config.setIndexedFields(indexedFields);

        Multimap<String,String> compositeToFieldMap = HashMultimap.create();
        compositeToFieldMap.put("COMPOSITE", "X");
        compositeToFieldMap.put("COMPOSITE", "Y");
        config.setCompositeToFieldMap(compositeToFieldMap);

        QueryModel queryModel = new QueryModel();
        queryModel.addTermToModel("FIELD_A", "FIELD_A");
        queryModel.addTermToModel("FIELD_A", "FIELD_B");
        config.setQueryModel(queryModel);

        helper.setIndexedFields(indexedFields);
    }

    @Test
    public void testNoData() throws Exception {
        // no data
        String query = "FIELD_A =~ 'ba.*'";
        // expected to see NOFIELD but the original was preserved for some reason...
        driveExpansion(query, query);
    }

    @Test
    public void testExpansionIntoSingleValue() throws Exception {
        write("bar", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoMultipleValues() throws Exception {
        write("bar", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        driveExpansion(query, expected);
    }

    @Test
    public void testExpansionIntoMultipleValuesSkippingFormerlyIndexedField() throws Exception {
        write("bar", "FIELD_A");
        write("bar", "FIELD_X");
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        driveExpansion(query, expected);
    }

    @Test
    public void testNegatedRegexExpandedByDefault() throws Exception {
        write("bar", "FIELD_A");
        String query = "!(FIELD_A =~ 'ba.*')";
        String expected = "!(FIELD_A == 'bar')";
        driveExpansion(query, expected);
    }

    @Test
    public void testRegexDoesNotExpandIntoCompositeFieldValue() throws Exception {
        // the functionality to remove composite fields should not be triggered by the RegexIndexExpansionVisitor.
        write("bar", "FIELD_A");
        write("baz", "COMPOSITE");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testUnfieldedRegexNotExpanded() throws Exception {
        // the RegexIndexExpansionVisitor should not execute an unfielded scan
        write("bar", "FIELD_A");
        String query = "_ANYFIELD_ =~ 'ba.*'";
        driveExpansion(query, query);
    }

    @Test
    public void testExpansionRestrictedToQueryModelForwardMappings() throws Exception {
        // query model forward mappings only has FIELD_A
        write("bar", "FIELD_A");
        write("bar", "FIELD_B");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar'";
        driveExpansion(query, expected);
    }

    @Test
    public void testValueExpansionThreshold() throws Exception {
        write("bar", "FIELD_A");
        write("bat", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "((_Value_ = true) && (FIELD_A =~ 'ba.*'))";
        config.setMaxValueExpansionThreshold(1);
        driveExpansion(query, expected);
    }

    @Test
    public void testKeyExpansionThreshold() throws Exception {
        write("bar", "FIELD_A");
        write("bat", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "((_Value_ = true) && (FIELD_A =~ 'ba.*'))";
        config.setMaxValueExpansionThreshold(1);
        driveExpansion(query, expected);
    }

    @Test
    public void testSimulatedTimeout() throws Exception {
        write("bar", "FIELD_A");
        write("bat", "FIELD_A", EXCEPTEDVALUE);
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "((_Value_ = true) && (FIELD_A =~ 'ba.*'))";
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeNoMatches() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A =~ 'ba.*'";
        config.setDatatypeFilter(Set.of("datatype-z"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypePartialMatch() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'baz'";
        config.setDatatypeFilter(Set.of("datatype-b"));
        driveExpansion(query, expected);
    }

    @Test
    public void testDataTypeFullMatch() throws Exception {
        write("bar", "FIELD_A", DEFAULT_DATE, "datatype-a");
        write("baz", "FIELD_A", DEFAULT_DATE, "datatype-b");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        config.setDatatypeFilter(Set.of("datatype-a", "datatype-b"));
        driveExpansion(query, expected);
    }

    @Override
    protected JexlNode expand(ASTJexlScript script) throws Exception {
        return RegexIndexExpansionVisitor.expandRegex(config, scannerFactory, helper, lookupMap, script);
    }
}
