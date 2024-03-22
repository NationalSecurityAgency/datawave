package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;

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

        // negated terms actually contribute to the total ingest types (union operation)
        assertSingleNode("B == '2' && !(C == '3')", aTypes);
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
        assertSingleNode("A == null", Collections.emptySet());
        assertSingleNode("!(A == null)", Collections.emptySet());
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

    private JexlNode parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException("Failed to parse query");
        }
    }
}
