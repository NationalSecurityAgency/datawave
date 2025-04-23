package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor;
import datawave.query.util.MockMetadataHelper;

class ContentFunctionsDescriptorTest {

    private final String unfieldedPhrase = "content:phrase(termOffsetMap, 'foo', 'bar')";
    private final String fieldedPhrase = "content:phrase(FIELD, termOffsetMap, 'foo', 'bar')";
    private final String multiFieldedPhrase = "content:phrase((FIELD_A || FIELD_B), termOffsetMap, 'foo', 'bar')";

    private final String unfieldedScoredPhrase = "content:scoredPhrase(-1.5, termOffsetMap, 'foo', 'bar')";
    private final String fieldedScoredPhrase = "content:scoredPhrase(FIELD, -1.5, termOffsetMap, 'foo', 'bar')";
    private final String multiFieldedScoredPhrase = "content:scoredPhrase((FIELD_A || FIELD_B), -1.5, termOffsetMap, 'foo', 'bar')";

    private final String unfieldedAdjacent = "content:adjacent(termOffsetMap, 'foo', 'bar')";
    private final String fieldedAdjacent = "content:adjacent(FIELD, termOffsetMap, 'foo', 'bar')";
    private final String multiFieldedAdjacent = "content:adjacent((FIELD_A || FIELD_B), termOffsetMap, 'foo', 'bar')";

    private final String unfieldedWithin = "content:within(1, termOffsetMap, 'foo', 'bar')";
    private final String fieldedWithin = "content:within(FIELD, 1, termOffsetMap, 'foo', 'bar')";
    private final String multiFieldedWithin = "content:within((FIELD_A || FIELD_B), 1, termOffsetMap, 'foo', 'bar')";

    private final ContentFunctionsDescriptor descriptor = new ContentFunctionsDescriptor();

    @Test
    void testGetHitTermValue() {
        String expected = "foo bar";
        assertHitTermValue(getDescriptor(unfieldedPhrase), expected);
        assertHitTermValue(getDescriptor(fieldedPhrase), expected);
        assertHitTermValue(getDescriptor(multiFieldedPhrase), expected);

        assertHitTermValue(getDescriptor(unfieldedScoredPhrase), expected);
        assertHitTermValue(getDescriptor(fieldedScoredPhrase), expected);
        assertHitTermValue(getDescriptor(multiFieldedScoredPhrase), expected);

        assertHitTermValue(getDescriptor(unfieldedAdjacent), expected);
        assertHitTermValue(getDescriptor(fieldedAdjacent), expected);
        assertHitTermValue(getDescriptor(multiFieldedAdjacent), expected);

        assertHitTermValue(getDescriptor(unfieldedWithin), expected);
        assertHitTermValue(getDescriptor(fieldedWithin), expected);
        assertHitTermValue(getDescriptor(multiFieldedWithin), expected);
    }

    private void assertHitTermValue(ContentJexlArgumentDescriptor jexlDescriptor, String expected) {
        assertEquals(expected, jexlDescriptor.getHitTermValue());
    }

    @Test
    void testGetHitTermValues() {
        Set<String> expectedHitTermValues = Set.of("foo", "bar");
        assertHitTermValues(getDescriptor(unfieldedPhrase), expectedHitTermValues);
        assertHitTermValues(getDescriptor(fieldedPhrase), expectedHitTermValues);
        assertHitTermValues(getDescriptor(multiFieldedPhrase), expectedHitTermValues);

        assertHitTermValues(getDescriptor(unfieldedScoredPhrase), expectedHitTermValues);
        assertHitTermValues(getDescriptor(fieldedScoredPhrase), expectedHitTermValues);
        assertHitTermValues(getDescriptor(multiFieldedScoredPhrase), expectedHitTermValues);

        assertHitTermValues(getDescriptor(unfieldedAdjacent), expectedHitTermValues);
        assertHitTermValues(getDescriptor(fieldedAdjacent), expectedHitTermValues);
        assertHitTermValues(getDescriptor(multiFieldedAdjacent), expectedHitTermValues);

        assertHitTermValues(getDescriptor(unfieldedWithin), expectedHitTermValues);
        assertHitTermValues(getDescriptor(fieldedWithin), expectedHitTermValues);
        assertHitTermValues(getDescriptor(multiFieldedWithin), expectedHitTermValues);
    }

    private void assertHitTermValues(ContentJexlArgumentDescriptor jexlDescriptor, Set<String> expected) {
        assertEquals(expected, jexlDescriptor.getHitTermValues());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFieldsAndTerms() {
        assertFieldsAndTerms(getDescriptor(unfieldedPhrase), Set.of(), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(fieldedPhrase), Set.of("FIELD"), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(multiFieldedPhrase), Set.of("FIELD_A", "FIELD_B"), Set.of("foo", "bar"));

        assertFieldsAndTerms(getDescriptor(unfieldedScoredPhrase), Set.of(), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(fieldedScoredPhrase), Set.of("FIELD"), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(multiFieldedScoredPhrase), Set.of("FIELD_A", "FIELD_B"), Set.of("foo", "bar"));

        assertFieldsAndTerms(getDescriptor(unfieldedAdjacent), Set.of(), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(fieldedAdjacent), Set.of("FIELD"), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(multiFieldedAdjacent), Set.of("FIELD_A", "FIELD_B"), Set.of("foo", "bar"));

        assertFieldsAndTerms(getDescriptor(unfieldedWithin), Set.of(), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(fieldedWithin), Set.of("FIELD"), Set.of("foo", "bar"));
        assertFieldsAndTerms(getDescriptor(multiFieldedWithin), Set.of("FIELD_A", "FIELD_B"), Set.of("foo", "bar"));
    }

    private void assertFieldsAndTerms(ContentJexlArgumentDescriptor jexlDescriptor, Set<String> fields, Set<String> terms) {
        ContentFunctionsDescriptor.FieldTerms fieldsAndTerms = jexlDescriptor.fieldsAndTerms(Set.of(), Set.of(), Set.of(), new MutableBoolean(true));
        assertEquals(fields, fieldsAndTerms.getFields());
        assertEquals(terms, fieldsAndTerms.getTerms());

        fieldsAndTerms = jexlDescriptor.fieldsAndTerms(Set.of(), Set.of(), Set.of(), new MutableBoolean(true), false);
        assertEquals(fields, fieldsAndTerms.getFields());
        assertEquals(terms, fieldsAndTerms.getTerms());
    }

    @Test
    void testFieldSets() {
        assertFieldSets(getDescriptor(unfieldedPhrase), Set.of(Set.of("FIELD"), Set.of("FIELD_A"), Set.of("FIELD_B")));
        assertFieldSets(getDescriptor(fieldedPhrase), Set.of(Set.of("FIELD")));
        assertFieldSets(getDescriptor(multiFieldedPhrase), Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(getDescriptor(unfieldedScoredPhrase), Set.of(Set.of("FIELD"), Set.of("FIELD_A"), Set.of("FIELD_B")));
        assertFieldSets(getDescriptor(fieldedScoredPhrase), Set.of(Set.of("FIELD")));
        assertFieldSets(getDescriptor(multiFieldedScoredPhrase), Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(getDescriptor(unfieldedAdjacent), Set.of(Set.of("FIELD"), Set.of("FIELD_A"), Set.of("FIELD_B")));
        assertFieldSets(getDescriptor(fieldedAdjacent), Set.of(Set.of("FIELD")));
        assertFieldSets(getDescriptor(multiFieldedAdjacent), Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

        assertFieldSets(getDescriptor(unfieldedWithin), Set.of(Set.of("FIELD"), Set.of("FIELD_A"), Set.of("FIELD_B")));
        assertFieldSets(getDescriptor(fieldedWithin), Set.of(Set.of("FIELD")));
        assertFieldSets(getDescriptor(multiFieldedWithin), Set.of(Set.of("FIELD_A"), Set.of("FIELD_B")));

    }

    private void assertFieldSets(ContentJexlArgumentDescriptor jexlDescriptor, Set<Set<String>> expected) {
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addTermFrequencyFields(Set.of("FIELD", "FIELD_A", "FIELD_B"));
        helper.setIndexedFields(Set.of("FIELD", "FIELD_A", "FIELD_B"));

        Set<Set<String>> fieldSet = jexlDescriptor.fieldSets(helper, Set.of());
        assertEquals(expected, fieldSet);
    }

    @Test
    void testFields() {
        assertFields(getDescriptor(unfieldedPhrase), Set.of("FIELD", "FIELD_A", "FIELD_B"));
        assertFields(getDescriptor(fieldedPhrase), Set.of("FIELD"));
        assertFields(getDescriptor(multiFieldedPhrase), Set.of("FIELD_A", "FIELD_B"));

        assertFields(getDescriptor(unfieldedScoredPhrase), Set.of("FIELD", "FIELD_A", "FIELD_B"));
        assertFields(getDescriptor(fieldedScoredPhrase), Set.of("FIELD"));
        assertFields(getDescriptor(multiFieldedScoredPhrase), Set.of("FIELD_A", "FIELD_B"));

        assertFields(getDescriptor(unfieldedAdjacent), Set.of("FIELD", "FIELD_A", "FIELD_B"));
        assertFields(getDescriptor(fieldedAdjacent), Set.of("FIELD"));
        assertFields(getDescriptor(multiFieldedAdjacent), Set.of("FIELD_A", "FIELD_B"));

        assertFields(getDescriptor(unfieldedWithin), Set.of("FIELD", "FIELD_A", "FIELD_B"));
        assertFields(getDescriptor(fieldedWithin), Set.of("FIELD"));
        assertFields(getDescriptor(multiFieldedWithin), Set.of("FIELD_A", "FIELD_B"));
    }

    private void assertFields(ContentJexlArgumentDescriptor jexlDescriptor, Set<String> expected) {
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addTermFrequencyFields(Set.of("FIELD", "FIELD_A", "FIELD_B"));
        helper.setIndexedFields(Set.of("FIELD", "FIELD_A", "FIELD_B"));

        Set<String> fieldSet = jexlDescriptor.fields(helper, Set.of());
        assertEquals(expected, fieldSet);
    }

    private ContentJexlArgumentDescriptor getDescriptor(String query) {
        return descriptor.getArgumentDescriptor(getFunction(query));
    }

    private ASTFunctionNode getFunction(String query) {
        ASTJexlScript script = getQueryTree(query);
        JexlNode node = script.jjtGetChild(0);
        if (node instanceof ASTFunctionNode) {
            return (ASTFunctionNode) node;
        }
        fail("Node was not an ASTFunctionNode");
        throw new RuntimeException("Node was not an ASTFunctionNode");
    }

    private ASTJexlScript getQueryTree(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }
}
