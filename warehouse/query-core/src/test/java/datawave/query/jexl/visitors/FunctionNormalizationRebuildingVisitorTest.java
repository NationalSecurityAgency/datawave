package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;

public class FunctionNormalizationRebuildingVisitorTest {

    private MetadataHelper helper;
    private Multimap<String,Type<?>> normalizers;

    @BeforeEach
    public void setup() {
        helper = new MockMetadataHelper();
        ((MockMetadataHelper) helper).addFields(Set.of("FOO", "NUM"));
        ((MockMetadataHelper) helper).addTermFrequencyFields(Set.of("FOO", "NUM"));

        normalizers = LinkedListMultimap.create();
        normalizers.putAll("FOO", List.of(new NoOpType(), new LcNoDiacriticsType()));
        normalizers.putAll("NUM", List.of(new NoOpType(), new NumberType()));
    }

    @Test
    public void testContentPhraseNoField_NormalizedTextValues() {
        String query = "content:phrase(termOffsetMap, 'bar', 'baz')";
        test(query, query);
    }

    @Test
    public void testContentPhraseNoField_TextValues() {
        String query = "content:phrase(termOffsetMap, 'Bar', 'baZ')";
        test(query, query);
    }

    @Test
    public void testContentPhraseNoField_TextAndNumericValues() {
        String query = "content:phrase(termOffsetMap, 'Bar', '23')";
        test(query, query);
    }

    @Test
    public void testContentPhraseNoField_NumericValues() {
        String query = "content:phrase(termOffsetMap, '12', '23')";
        test(query, query);
    }

    @Test
    public void testContentPhraseSingleField_NormalizedTextValues() {
        String query = "content:phrase(FOO, termOffsetMap, 'bar', 'baz')";
        test(query, query);

        query = "content:phrase(NUM, termOffsetMap, 'bar', 'baz')";
        test(query, query);
    }

    @Test
    public void testContentPhraseSingleField_TextValues() {
        String query = "content:phrase(FOO, termOffsetMap, 'Bar', 'baZ')";
        test(query, "(content:phrase(FOO, termOffsetMap, 'Bar', 'baZ') || content:phrase(FOO, termOffsetMap, 'bar', 'baz'))");

        query = "content:phrase(NUM, termOffsetMap, 'Bar', 'baZ')";
        test(query, query);
    }

    @Test
    public void testContentPhraseSingleField_TextAndNumericValues() {
        String query = "content:phrase(FOO, termOffsetMap, 'Bar', '23')";
        test(query, "(content:phrase(FOO, termOffsetMap, 'Bar', '23') || content:phrase(FOO, termOffsetMap, 'bar', '23'))");

        query = "content:phrase(NUM, termOffsetMap, 'Bar', '23')";
        test(query, query);
    }

    @Test
    public void testContentPhraseSingleField_NumericValues() {
        String query = "content:phrase(FOO, termOffsetMap, '12', '23')";
        test(query, query);

        query = "content:phrase(NUM, termOffsetMap, '12', '23')";
        test(query, "(content:phrase(NUM, termOffsetMap, '+bE1.2', '+bE2.3') || content:phrase(NUM, termOffsetMap, '12', '23'))");
    }

    @Test
    public void testContentPhraseMultiField_NormalizedTextValues() {
        String query = "content:phrase((FOO || NUM), termOffsetMap, 'bar', 'baz')";
        test(query, query);
    }

    @Test
    public void testContentPhraseMultiField_TextValues() {
        String query = "content:phrase((FOO || NUM), termOffsetMap, 'Bar', 'baZ')";
        String expected = "(content:phrase((FOO || NUM), termOffsetMap, 'Bar', 'baZ') || content:phrase((FOO || NUM), termOffsetMap, 'bar', 'baz'))";
        test(query, expected);
    }

    @Test
    public void testContentPhraseMultiField_TextAndNumericValues() {
        String query = "content:phrase((FOO || NUM), termOffsetMap, 'Bar', '23')";
        String expected = "(content:phrase((FOO || NUM), termOffsetMap, 'Bar', '23') || content:phrase((FOO || NUM), termOffsetMap, 'bar', '23'))";
        test(query, expected);
        // note: first argument normalizes correctly, second argument fails normalization
    }

    @Test
    public void testContentPhraseMultiField_NumericValues() {
        String query = "content:phrase((FOO || NUM), termOffsetMap, '12', '23')";
        String expected = "(content:phrase((FOO || NUM), termOffsetMap, '+bE1.2', '+bE2.3') || content:phrase((FOO || NUM), termOffsetMap, '12', '23'))";
        test(query, expected);
    }

    @Test
    public void testIncludeText() {
        String query = "f:includeText(FOO, 'bar')";
        test(query, query);

        query = "f:includeText(FOO, 'BaR')";
        test(query, query);

        query = "f:includeText(NUM, '23')";
        test(query, query);
    }

    @Test
    public void testMatchRegex() {
        String query = "f:matchRegex(FOO, 'ba*')";
        test(query, query);

        query = "f:matchRegex(FOO, '[A-Z]+')";
        String expected = "(f:matchRegex(FOO, '[A-Z]+') || f:matchRegex(FOO, '[a-z]+'))";
        test(query, expected);

        query = "f:matchRegex(NUM, '23.*')";
        expected = "(f:matchRegex(NUM, '23.*') || f:matchRegex(NUM, '\\\\+[b-z]E2\\\\.3.*'))";
        test(query, expected);
        // note: this seems wrong
    }

    private void test(String query, String expected) {
        ASTJexlScript script = getQuery(query);
        JexlNode child = script.jjtGetChild(0);

        assertInstanceOf(ASTFunctionNode.class, child);

        JexlNode node = FunctionNormalizationRebuildingVisitor.normalize((ASTFunctionNode) child, normalizers, helper, Collections.emptySet());
        String result = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(expected, result);
    }

    private ASTJexlScript getQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (Exception e) {
            fail("Failed to parse query: " + query);
            throw new IllegalArgumentException("Bad parse");
        }
    }
}
