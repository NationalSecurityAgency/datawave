package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.data.type.LcType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;

class IngestTypeVisitorTest {

    private static final TypeMetadata typeMetadata = new TypeMetadata();

    private final Set<String> aTypes = Sets.newHashSet("ingestType1", "ingestType2", "ingestType3", "ingestType4", "ingestType5", "ingestType6", "ingestType7");
    private final Set<String> bTypes = Sets.newHashSet("ingestType1", "ingestType3", "ingestType5", "ingestType7");
    private final Set<String> cTypes = Sets.newHashSet("ingestType2", "ingestType4", "ingestType6");
    private final Set<String> xType = Collections.singleton("ingestType1");
    private final Set<String> yType = Collections.singleton("ingestType2");
    private final Set<String> zType = Collections.singleton("ingestType3");

    @BeforeAll
    public static void setup() {
        // A is 1 to 7
        typeMetadata.put("A", "ingestType1", LcType.class.getName());
        typeMetadata.put("A", "ingestType2", LcType.class.getName());
        typeMetadata.put("A", "ingestType3", LcType.class.getName());
        typeMetadata.put("A", "ingestType4", LcType.class.getName());
        typeMetadata.put("A", "ingestType5", LcType.class.getName());
        typeMetadata.put("A", "ingestType6", LcType.class.getName());
        typeMetadata.put("A", "ingestType7", LcType.class.getName());
        // B is odds 1 to 7
        typeMetadata.put("B", "ingestType1", LcType.class.getName());
        typeMetadata.put("B", "ingestType3", LcType.class.getName());
        typeMetadata.put("B", "ingestType5", LcType.class.getName());
        typeMetadata.put("B", "ingestType7", LcType.class.getName());
        // C is evens 1 to 7
        typeMetadata.put("C", "ingestType2", LcType.class.getName());
        typeMetadata.put("C", "ingestType4", LcType.class.getName());
        typeMetadata.put("C", "ingestType6", LcType.class.getName());
        // X is 1
        typeMetadata.put("X", "ingestType1", LcType.class.getName());
        // Y is 2
        typeMetadata.put("Y", "ingestType2", LcType.class.getName());
        // Z is 3
        typeMetadata.put("Z", "ingestType3", LcType.class.getName());
    }

    @Test
    void testSingleTerms() {
        String query = "A == '1'";
        assertSingleNode(query, aTypes);

        query = "B == '2'";
        assertSingleNode(query, bTypes);

        query = "C == '3'";
        assertSingleNode(query, cTypes);

        query = "X == '7'";
        assertSingleNode(query, xType);

        query = "Y == '8'";
        assertSingleNode(query, yType);

        query = "Z == '9'";
        assertSingleNode(query, zType);
    }

    @Test
    void testUnionLogic() {
        // all types OR some types is still all types
        //  @formatter:off
        String[] queries = {
                        "A == '1' || B == '2'",
                        "A == '1' || C == '3'",
                        "A == '1' || X == '1'",
                        "A == '1' || Y == '2'",
                        "A == '1' || Z == '3'",
        };
        //  @formatter:on

        for (String query : queries) {
            assertSingleNode(query, aTypes);
        }

        // assert evens OR odds is all types
        assertSingleNode("B == '2' || C == '3'", aTypes);

        // assert odds OR single type terms
        assertSingleNode("B == '2' || X == '1'", bTypes);
        assertSingleNode("B == '2' || Y == '1'", Sets.union(bTypes, yType));
        assertSingleNode("B == '2' || Z == '3'", bTypes);

        // assert evens OR single type terms
        assertSingleNode("C == '3' || X == '1'", Sets.union(cTypes, xType));
        assertSingleNode("C == '3' || Y == '1'", cTypes);
        assertSingleNode("C == '3' || Z == '3'", Sets.union(cTypes, zType));
    }

    @Test
    void testIntersectionLogic() {
        assertSingleNode("A == '1' && B == '2'", Sets.intersection(aTypes, bTypes));
        assertSingleNode("A == '1' && C == '3'", Sets.intersection(aTypes, cTypes));
        assertSingleNode("A == '1' && X == '1'", xType);
        assertSingleNode("A == '1' && Y == '2'", yType);
        assertSingleNode("A == '1' && Z == '3'", zType);

        // also cover cases were no intersection is possible
        assertSingleNode("B == '2' && Y == '2'", Collections.emptySet());
        assertSingleNode("C == '3' && X == '1'", Collections.emptySet());
    }

    @Test
    void testNestedJunctionLogic() {
        assertSingleNode("X == '1' && (Y == '2' || Z == '3')", Collections.emptySet());
        assertSingleNode("X == '1' || (Y == '2' && Z == '3')", xType);
    }

    @Test
    void testNegatedLogic() {
        // negated terms do not contribute to an intersection
        assertSingleNode("A == '1' && !(B == '2')", aTypes);
        assertSingleNode("B == '2' && !(C == '3')", bTypes);
    }

    @Test
    void testUnknownType() {
        assertSingleNode("A == '1' && D == '4'", Collections.emptySet());
        assertSingleNode("A == '1' || D == '4'", Collections.emptySet());
        assertSingleNode("D == '4' && E == '5'", Collections.emptySet());
        assertSingleNode("D == '4' || E == '5'", Collections.emptySet());
    }

    @Test
    void testEqNull() {
        assertSingleNode("A == null", aTypes);
        assertSingleNode("!(A == null)", aTypes);
    }

    @Test
    void testNotNullAndNestedUnion() {
        String query = "!(A == null) && B == '1' && ((C == '2' || D == '2' || E == '2' || F == '2'))";

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("A", "type2", LcType.class.getTypeName());
        metadata.put("A", "type3", LcType.class.getTypeName());
        metadata.put("A", "type4", LcType.class.getTypeName());

        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("B", "type3", LcType.class.getTypeName());
        metadata.put("B", "type4", LcType.class.getTypeName());

        metadata.put("C", "type1", LcType.class.getTypeName());
        metadata.put("C", "type2", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("C", "type4", LcType.class.getTypeName());
        metadata.put("C", "type5", LcType.class.getTypeName());
        metadata.put("C", "type6", LcType.class.getTypeName());

        metadata.put("D", "type1", LcType.class.getTypeName());
        metadata.put("D", "type2", LcType.class.getTypeName());
        metadata.put("D", "type3", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("D", "type5", LcType.class.getTypeName());
        metadata.put("D", "type6", LcType.class.getTypeName());

        metadata.put("E", "type3", LcType.class.getTypeName());
        metadata.put("F", "type3", LcType.class.getTypeName());

        Set<String> expected = Sets.newHashSet("type1", "type2", "type3", "type4");
        test(query, expected, metadata);
    }

    @Test
    void testFilterFunctionExclude() {
        String query = "((A == '1' || A == '2' || A == '3' || A =='4' || A == '5') && B == '1' && C == '1' && (!filter:includeRegex(D,'bar')))";

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("A", "type2", LcType.class.getTypeName());

        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("B", "type3", LcType.class.getTypeName());
        metadata.put("B", "type4", LcType.class.getTypeName());
        metadata.put("B", "type5", LcType.class.getTypeName());
        metadata.put("B", "type6", LcType.class.getTypeName());

        metadata.put("C", "type1", LcType.class.getTypeName());
        metadata.put("C", "type2", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("C", "type4", LcType.class.getTypeName());
        metadata.put("C", "type5", LcType.class.getTypeName());
        metadata.put("C", "type6", LcType.class.getTypeName());

        metadata.put("D", "type1", LcType.class.getTypeName());
        metadata.put("D", "type2", LcType.class.getTypeName());
        metadata.put("D", "type3", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("D", "type5", LcType.class.getTypeName());
        metadata.put("D", "type6", LcType.class.getTypeName());

        Set<String> expected = Sets.newHashSet("type1", "type2");
        test(query, expected, metadata);
    }

    @Test
    void testWrappedStuff() {
        String query = "!(A == null) && B == '1' && (!(C == null))";

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("C", "type1", LcType.class.getTypeName());

        test(query, Collections.singleton("type1"), metadata);
    }

    @Test
    void testAndNull() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("9", "ingestType2", LcType.class.getTypeName());

        String query = "A == '1' && B == null"; // *technically* valid
        test(query, Collections.singleton("ingestType1"), metadata);

        // same form but with an identifier
        query = "A == '1' && $9 == null";
        test(query, Collections.singleton("ingestType1"), metadata);
    }

    @Test
    void testAndNotNull() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("9", "ingestType2", LcType.class.getTypeName());

        // in theory this visitor should be smart enough to prune out a negation for an exclusive ingest type
        String query = "A == '1' && !(B == null)";
        test(query, Collections.singleton("ingestType1"), metadata);

        query = "A == '1' && !($B == null)";
        test(query, Collections.singleton("ingestType1"), metadata);
    }

    @Test
    void testAndMultiFieldedDateFilter() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("C", "type1", LcType.class.getTypeName());
        metadata.put("D", "type1", LcType.class.getTypeName());
        metadata.put("1", "type1", LcType.class.getTypeName());
        metadata.put("2", "type1", LcType.class.getTypeName());
        metadata.put("3", "type1", LcType.class.getTypeName());

        String query = "!(A == null) && ($1 == '1' || B == '1') && ($2 == '2' || C == '2') && filter:afterDate(($3 || D), '2024-01-01')";
        test(query, Collections.singleton("type1"), metadata);
    }

    private void assertSingleNode(String query, Set<String> expectedIngestTypes) {
        assertSingleNode(query, expectedIngestTypes, typeMetadata);
    }

    private void assertSingleNode(String query, Set<String> expectedIngestTypes, TypeMetadata typeMetadata) {
        JexlNode node = parseQuery(query);
        Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(node, typeMetadata);
        assertEquals(expectedIngestTypes, ingestTypes);
    }

    private void assertJunction(String query, Set<String>[] expectedIngestTypes) {
        assertJunction(query, expectedIngestTypes, typeMetadata);
    }

    private void assertJunction(String query, Set<String>[] expectedIngestTypes, TypeMetadata typeMetadata) {
        JexlNode node = parseQuery(query);

        assertEquals(expectedIngestTypes.length, node.jjtGetNumChildren(), "Child array and expected type array had differing lengths!");
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(child, typeMetadata);
            assertEquals(expectedIngestTypes[i], ingestTypes);
        }
    }

    private void test(String query, Set<String> expected) {
        test(query, expected, typeMetadata);
    }

    private void test(String query, Set<String> expected, TypeMetadata typeMetadata) {
        ASTJexlScript script = parseQuery(query);
        Set<String> types = IngestTypeVisitor.getIngestTypes(script, typeMetadata);
        assertEquals(new TreeSet<>(expected), new TreeSet<>(types)); // sorted sets make differences easy to spot
    }

    private ASTJexlScript parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException("Failed to parse query");
        }
    }
}
