package datawave.query.jexl.functions;

import static datawave.query.jexl.functions.QueryFunctionsDescriptor.QueryJexlArgumentDescriptor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;

/**
 * Although most query functions are removed from the query by the {@link QueryOptionsFromQueryVisitor}, several functions will persist. These functions may
 * contribute contextual information to the query planner, namely what fields are present in the query. When a field only exists in one of these non-removable
 * functions it is important to verify that all fields are actually parsed by the {@link QueryFunctionsDescriptor}.
 */
class QueryFunctionsDescriptorTest {

    private final String singleFieldCount = "f:count(FIELD)";
    private final String multiFieldedCount = "f:count(FIELD_A, FIELD_B)";

    private final String betweenDecimal = "f:between(FIELD, 50.0, 60.0)";
    private final String betweenValue = "f:between(FIELD, 'm', 'm~')";

    private final String length = "f:length(FIELD, '2', '3')";

    private final String include = "f:includeText(FIELD, 'baz')";
    private final String includeAnd = "f:includeText(AND, FIELD_A, 'bar', FIELD_B, 'baz')";
    private final String includeOr = "f:includeText(OR, FIELD_A, 'bar', FIELD_B, 'baz')";

    private final String regex = "f:matchRegex(FIELD, 'ba.*')";

    private final String singleFieldSum = "f:sum(FIELD)";
    private final String multiFieldSum = "f:sum(FIELD_A, FIELD_B)";

    private final String singleFieldMin = "f:min(FIELD)";
    private final String multiFieldMin = "f:min(FIELD_A, FIELD_B)";

    private final String singleFieldMax = "f:max(FIELD)";
    private final String multiFieldMax = "f:max(FIELD_A, FIELD_B)";

    private final String singleFieldAvg = "f:average(FIELD)";
    private final String multiFieldAvg = "f:average(FIELD_A, FIELD_B)";

    private final String singleFieldGroupBy = "f:groupby(FIELD)";
    private final String multiFieldGroupBy = "f:groupby(FIELD_A, FIELD_B)";

    private final String singleFieldUnique = "f:unique(FIELD)";
    private final String multiFieldUnique = "f:unique(FIELD_A, FIELD_B)";

    private final String singleFieldUniqueDay = "f:unique('FIELD[DAY]')";
    private final String multiFieldUniqueDay = "f:unique('FIELD_A[DAY]', 'FIELD_B[DAY]')";

    private final String singleFieldNoExpansion = "f:noExpansion(FIELD)";
    private final String multiFieldNoExpansion = "f:noExpansion(FIELD_A, FIELD_B)";

    private final String singleFieldLenient = "f:lenient(FIELD)";
    private final String multiFieldLenient = "f:lenient(FIELD_A, FIELD_B)";

    private final String singleFieldStrict = "f:strict(FIELD)";
    private final String multiFieldStrict = "f:strict(FIELD_A, FIELD_B)";

    private final QueryFunctionsDescriptor descriptor = new QueryFunctionsDescriptor();

    private ShardQueryConfiguration config;
    private MockMetadataHelper helper;
    private DateIndexHelper dateIndexHelper;

    @BeforeEach
    public void setup() {
        config = new ShardQueryConfiguration();

        Multimap<String,Type<?>> fieldToTypes = HashMultimap.create();
        fieldToTypes.putAll("FIELD", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType(), new NoOpType()));
        fieldToTypes.putAll("FIELD_A", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType(), new NoOpType()));
        fieldToTypes.putAll("FIELD_B", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType(), new NoOpType()));

        helper = new MockMetadataHelper();
        helper.setDataTypes(fieldToTypes);

        dateIndexHelper = new MockDateIndexHelper();
    }

    @Test
    void testFields() {
        assertFields(singleFieldCount, Set.of("FIELD"));
        assertFields(multiFieldedCount, Set.of("FIELD_A", "FIELD_B"));

        assertFields(betweenDecimal, Set.of("FIELD"));
        assertFields(betweenValue, Set.of("FIELD"));

        assertFields(length, Set.of("FIELD"));

        assertFields(include, Set.of("FIELD"));
        assertFields(includeAnd, Set.of("FIELD_A", "FIELD_B"));
        assertFields(includeOr, Set.of("FIELD_A", "FIELD_B"));

        assertFields(regex, Set.of("FIELD"));

        assertFields(singleFieldSum, Set.of("FIELD"));
        assertFields(multiFieldSum, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldMin, Set.of("FIELD"));
        assertFields(multiFieldMin, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldMax, Set.of("FIELD"));
        assertFields(multiFieldMax, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldAvg, Set.of("FIELD"));
        assertFields(multiFieldAvg, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldGroupBy, Set.of("FIELD"));
        assertFields(multiFieldGroupBy, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldUnique, Set.of("FIELD"));
        assertFields(multiFieldUnique, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldUniqueDay, Set.of("FIELD"));
        assertFields(multiFieldUniqueDay, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldNoExpansion, Set.of("FIELD"));
        assertFields(multiFieldNoExpansion, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldLenient, Set.of("FIELD"));
        assertFields(multiFieldLenient, Set.of("FIELD_A", "FIELD_B"));

        assertFields(singleFieldStrict, Set.of("FIELD"));
        assertFields(multiFieldStrict, Set.of("FIELD_A", "FIELD_B"));
    }

    private void assertFields(String query, Set<String> expected) {
        QueryJexlArgumentDescriptor jexlDescriptor = getDescriptor(query);
        Set<String> fields = jexlDescriptor.fields(null, Set.of());
        assertEquals(expected, fields);
    }

    @Test
    void testFieldSets() {
        assertFieldSets(singleFieldCount, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldedCount, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(betweenDecimal, Set.of(Set.of("FIELD")));
        assertFieldSets(betweenValue, Set.of(Set.of("FIELD")));

        assertFieldSets(length, Set.of(Set.of("FIELD")));

        assertFieldSets(include, Set.of(Set.of("FIELD")));
        assertFieldSets(includeAnd, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));
        assertFieldSets(includeOr, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(regex, Set.of(Set.of("FIELD")));

        assertFieldSets(singleFieldSum, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldSum, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldMin, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldMin, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldMax, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldMax, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldAvg, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldAvg, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldGroupBy, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldGroupBy, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldUnique, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldUnique, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFields(singleFieldUniqueDay, Set.of("FIELD"));
        assertFields(multiFieldUniqueDay, Set.of("FIELD_A", "FIELD_B"));

        assertFieldSets(singleFieldNoExpansion, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldNoExpansion, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldLenient, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldLenient, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(singleFieldStrict, Set.of(Set.of("FIELD")));
        assertFieldSets(multiFieldStrict, Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));
    }

    private void assertFieldSets(String query, Set<Set<String>> expected) {
        QueryJexlArgumentDescriptor jexlDescriptor = getDescriptor(query);
        Set<Set<String>> fields = jexlDescriptor.fieldSets(null, Set.of());
        assertEquals(expected, fields);
    }

    private QueryJexlArgumentDescriptor getDescriptor(String query) {
        ASTJexlScript script = getQuery(query);
        JexlNode child = script.jjtGetChild(0);
        if (child instanceof ASTFunctionNode) {
            return (QueryJexlArgumentDescriptor) descriptor.getArgumentDescriptor((ASTFunctionNode) child);
        }
        throw new IllegalArgumentException("Could not get descriptor for query: " + query);
    }

    private ASTJexlScript getQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Could not parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testIndexQuery() {
        // test default functions
        assertIndexQuery(include, "FIELD == 'baz'");
        assertIndexQuery(includeAnd, "(FIELD_A == 'bar' && FIELD_B == 'baz')");
        assertIndexQuery(includeOr, "(FIELD_A == 'bar' || FIELD_B == 'baz')");

        // test fielded normalizations
        assertIndexQuery("f:includeText(FIELD, 'abc')", "FIELD == 'abc'");
        assertIndexQuery("f:includeText(FIELD_A, 'BaZ')", "(FIELD_A == 'BaZ' || FIELD_A == 'baz')");
        assertIndexQuery("f:includeText(FIELD_B, '123')", "(FIELD_B == '123' || FIELD_B == '+cE1.23')");

        // test non-fielded normalizations
        assertIndexQuery("f:includeText(_ANYFIELD_, 'abc')", "_ANYFIELD_ == 'abc'");
        assertIndexQuery("f:includeText(_ANYFIELD_, 'BaZ')", "(_ANYFIELD_ == 'BaZ' || _ANYFIELD_ == 'baz')");
        assertIndexQuery("f:includeText(_ANYFIELD_, '123')", "(_ANYFIELD_ == '123' || _ANYFIELD_ == '+cE1.23')");
    }

    private void assertIndexQuery(String query, String expected) {
        QueryJexlArgumentDescriptor argDescriptor = getDescriptor(query);
        JexlNode expanded = argDescriptor.getIndexQuery(config, helper, dateIndexHelper, Collections.emptySet());
        String result = JexlStringBuildingVisitor.buildQuery(expanded);
        assertEquals(expected, result);
    }
}
