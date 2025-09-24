package datawave.query.jexl.visitors;

import static datawave.core.iterators.TimeoutExceptionIterator.EXCEPTEDVALUE;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.model.QueryModel;
import datawave.util.time.DateHelper;

/**
 * Collection of tests for the {@link RegexIndexExpansionVisitor}
 */
public class RegexIndexExpansionVisitorIT extends BaseIndexExpansionTest {

    // FIELD_B not indexed
    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_C", "COMPOSITE");

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

        config.setBeginDate(DateHelper.parse("20250606"));
        config.setEndDate(DateHelper.parse("20250607"));

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
    public void testZeroTimeout() throws Exception {
        write("bar", "FIELD_A");
        write("bat", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "((_Value_ = true) && (FIELD_A =~ 'ba.*'))";
        config.setMaxIndexScanTimeMillis(0L);
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

    @Test
    public void testIgnoreRegexBasedOnCost() throws Exception {
        //  @formatter:off
        helper.setCardinalities(ImmutableMap.of(
                Maps.immutableEntry("FIELD_A", DEFAULT_DATE), ImmutableMap.of("datatype", 10_000L),
                Maps.immutableEntry("FIELD_C", DEFAULT_DATE), ImmutableMap.of("datatype", 600L)
        ));
        //  @formatter:on

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("FIELD_A", new LcNoDiacriticsType());
        dataTypes.put("FIELD_C", new LcNoDiacriticsType());
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        write("bar", "FIELD_A");
        write("baz", "FIELD_A");
        String query = "(FIELD_A =~ 'ba.*' || FIELD_A == 'other') && FIELD_C == 'value'";
        String expected = "(((_Delayed_ = true) && (FIELD_A =~ 'ba.*')) || FIELD_A == 'other') && FIELD_C == 'value'";
        driveExpansion(query, expected);
    }

    @Override
    protected JexlNode expand(ASTJexlScript script) throws Exception {
        return RegexIndexExpansionVisitor.expandRegex(config, scannerFactory, helper, lookupMap, script);
    }
}
