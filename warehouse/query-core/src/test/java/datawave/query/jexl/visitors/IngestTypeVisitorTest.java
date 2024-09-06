package datawave.query.jexl.visitors;

import static datawave.query.jexl.visitors.IngestTypeVisitor.IGNORED_TYPE;
import static datawave.query.jexl.visitors.IngestTypeVisitor.UNKNOWN_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.HashSet;
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
import datawave.query.jexl.util.JexlQueryGenerator;
import datawave.query.util.TypeMetadata;

class IngestTypeVisitorTest {

    private static final TypeMetadata typeMetadata = new TypeMetadata();

    private final Set<String> aTypes = Sets.newHashSet("ingestType1", "ingestType2", "ingestType3", "ingestType4", "ingestType5", "ingestType6", "ingestType7");
    private final Set<String> bTypes = Sets.newHashSet("ingestType1", "ingestType3", "ingestType5", "ingestType7");
    private final Set<String> cTypes = Sets.newHashSet("ingestType2", "ingestType4", "ingestType6");
    private final Set<String> xType = Collections.singleton("ingestType1");
    private final Set<String> yType = Collections.singleton("ingestType2");
    private final Set<String> zType = Collections.singleton("ingestType3");
    private final Set<String> identifierType = Collections.singleton("ingestType1");

    // special types
    private final Set<String> unknownType = Set.of(UNKNOWN_TYPE);
    private final Set<String> ignoredType = Set.of(IGNORED_TYPE);

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
        // $123 and $234 as identifiers
        typeMetadata.put("123", "ingestType1", LcType.class.getName());
        typeMetadata.put("234", "ingestType1", LcType.class.getName());
    }

    @Test
    void testLeafEq() {
        // EQ node with known field
        assertStandard("A == '1'", aTypes);
        assertInternal("A == '1'", aTypes);
        assertExternal("A == '1'", aTypes);

        // EQ node with known identifier
        assertStandard("$123 == '1'", identifierType);
        assertInternal("$123 == '1'", identifierType);
        assertExternal("$123 == '1'", identifierType);

        // EQ node with unknown field
        assertStandard("D == '1'", Set.of(UNKNOWN_TYPE));
        assertInternal("D == '1'", Set.of(UNKNOWN_TYPE));
        assertExternal("D == '1'", Set.of(UNKNOWN_TYPE));

        // EQ node with unknown identifier
        assertStandard("$987 == '1'", Set.of(UNKNOWN_TYPE));
        assertInternal("$987 == '1'", Set.of(UNKNOWN_TYPE));
        assertExternal("$987 == '1'", Set.of(UNKNOWN_TYPE));
    }

    @Test
    void testLeafNe() {
        // NE node with known field
        assertStandard("A != '1'", aTypes);
        assertInternal("A != '1'", aTypes);
        assertExternal("A != '1'", aTypes);

        // NE node with known identifier
        assertStandard("$123 != '1'", identifierType);
        assertInternal("$123 != '1'", identifierType);
        assertExternal("$123 != '1'", identifierType);

        // NE node with unknown field
        assertStandard("D != '1'", Set.of(UNKNOWN_TYPE));
        assertInternal("D != '1'", Set.of(UNKNOWN_TYPE));
        assertExternal("D != '1'", Set.of(UNKNOWN_TYPE));

        // NE node with unknown identifier
        assertStandard("$987 != '1'", Set.of(UNKNOWN_TYPE));
        assertInternal("$987 != '1'", Set.of(UNKNOWN_TYPE));
        assertExternal("$987 != '1'", Set.of(UNKNOWN_TYPE));
    }

    @Test
    void testLeafEr() {
        // ER node with known field
        assertStandard("A =~ 'ba.*'", aTypes);
        assertInternal("A =~ 'ba.*'", aTypes);
        assertExternal("A =~ 'ba.*'", aTypes);

        // ER node with known identifier
        assertStandard("$123 =~ 'ba.*'", identifierType);
        assertInternal("$123 =~ 'ba.*'", identifierType);
        assertExternal("$123 =~ 'ba.*'", identifierType);

        // ER node with unknown field
        assertStandard("D =~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertInternal("D =~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertExternal("D =~ 'ba.*'", Set.of(UNKNOWN_TYPE));

        // ER node with unknown identifier
        assertStandard("$987 =~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertInternal("$987 =~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertExternal("$987 =~ 'ba.*'", Set.of(UNKNOWN_TYPE));
    }

    @Test
    void testLeafNr() {
        // NR node with known field
        assertStandard("A !~ 'ba.*'", aTypes);
        assertInternal("A !~ 'ba.*'", aTypes);
        assertExternal("A !~ 'ba.*'", aTypes);

        // NR node with known identifier
        assertStandard("$123 !~ 'ba.*'", identifierType);
        assertInternal("$123 !~ 'ba.*'", identifierType);
        assertExternal("$123 !~ 'ba.*'", identifierType);

        // NR node with unknown field
        assertStandard("D !~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertInternal("D !~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertExternal("D !~ 'ba.*'", Set.of(UNKNOWN_TYPE));

        // NR node with unknown identifier
        assertStandard("$987 !~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertInternal("$987 !~ 'ba.*'", Set.of(UNKNOWN_TYPE));
        assertExternal("$987 !~ 'ba.*'", Set.of(UNKNOWN_TYPE));
    }

    @Test
    void testLeafGT() {
        // GT node with known field
        assertStandard("A > '1'", aTypes);
        assertInternal("A > '1'", aTypes);
        assertExternal("A > '1'", aTypes);

        // GT node with known identifier
        assertStandard("$123 > '1'", identifierType);
        assertInternal("$123 > '1'", identifierType);
        assertExternal("$123 > '1'", identifierType);

        // GT node with unknown field
        assertStandard("D > '1'", unknownType);
        assertInternal("D > '1'", unknownType);
        assertExternal("D > '1'", unknownType);

        // GT node with missing identifier
        assertStandard("$987 > '1'", unknownType);
        assertInternal("$987 > '1'", unknownType);
        assertExternal("$987 > '1'", unknownType);
    }

    @Test
    void testLeafLT() {
        // LT node with known field
        assertStandard("A < '1'", aTypes);
        assertInternal("A < '1'", aTypes);
        assertExternal("A < '1'", aTypes);

        // LT node with known identifier
        assertStandard("$123 < '1'", identifierType);
        assertInternal("$123 < '1'", identifierType);
        assertExternal("$123 < '1'", identifierType);

        // LT node with unknown field
        assertStandard("D < '1'", unknownType);
        assertInternal("D < '1'", unknownType);
        assertExternal("D < '1'", unknownType);

        // LT node with unknown identifier
        assertStandard("$987 < '1'", unknownType);
        assertInternal("$987 < '1'", unknownType);
        assertExternal("$987 < '1'", unknownType);
    }

    @Test
    void testLeafGE() {
        // GE node with known field
        assertStandard("A >= '1'", aTypes);
        assertInternal("A >= '1'", aTypes);
        assertExternal("A >= '1'", aTypes);

        // GE node with known identifier
        assertStandard("$123 >= '1'", identifierType);
        assertInternal("$123 >= '1'", identifierType);
        assertExternal("$123 >= '1'", identifierType);

        // GE node with unknown field
        assertStandard("D >= '1'", unknownType);
        assertInternal("D >= '1'", unknownType);
        assertExternal("D >= '1'", unknownType);

        // GE node with unknown identifier
        assertStandard("$987 >= '1'", unknownType);
        assertInternal("$987 >= '1'", unknownType);
        assertExternal("$987 >= '1'", unknownType);
    }

    @Test
    void testLeafLE() {
        // LE node with known field
        assertStandard("A <= '1'", aTypes);
        assertInternal("A <= '1'", aTypes);
        assertExternal("A <= '1'", aTypes);

        // LE node with known identifier
        assertStandard("$123 <= '1'", identifierType);
        assertInternal("$123 <= '1'", identifierType);
        assertExternal("$123 <= '1'", identifierType);

        // LE node with unknown field
        assertStandard("D <= '1'", unknownType);
        assertInternal("D <= '1'", unknownType);
        assertExternal("D <= '1'", unknownType);

        // LE node with unknown identifier
        assertStandard("$987 <= '1'", unknownType);
        assertInternal("$987 <= '1'", unknownType);
        assertExternal("$987 <= '1'", unknownType);
    }

    @Test
    void testNot() {
        // Not node with known field
        assertStandard("!(A == '1')", Set.of(IGNORED_TYPE));
        assertInternal("!(A == '1')", Set.of(IGNORED_TYPE));
        assertExternal("!(A >= '1')", aTypes);

        // Not node with known identifier
        assertStandard("!($123 == '1')", Set.of(IGNORED_TYPE));
        assertInternal("!($123 == '1')", Set.of(IGNORED_TYPE));
        assertExternal("!($123 >= '1')", identifierType);

        // Not node with unknown field
        assertStandard("!(D == '1')", Set.of(IGNORED_TYPE));
        assertInternal("!(D == '1')", Set.of(IGNORED_TYPE));
        assertExternal("!(D >= '1')", unknownType);

        // Not node with unknown identifier
        assertStandard("!($987 == '1')", Set.of(IGNORED_TYPE));
        assertInternal("!($987 == '1')", Set.of(IGNORED_TYPE));
        assertExternal("!($987 >= '1')", unknownType);
    }

    @Test
    void testNullLiteral() {
        // null literal with known field
        assertStandard("A == null", Set.of(IGNORED_TYPE));
        assertInternal("A == null", Set.of(IGNORED_TYPE));
        assertExternal("A == null", aTypes);

        // null literal with known identifier
        assertStandard("$123 == null", Set.of(IGNORED_TYPE));
        assertInternal("$123 == null", Set.of(IGNORED_TYPE));
        assertExternal("$123 == null", identifierType);

        // null literal with unknown field
        assertStandard("D == null", Set.of(IGNORED_TYPE));
        assertInternal("D == null", Set.of(IGNORED_TYPE));
        assertExternal("D == null", unknownType);

        // null literal with unknown identifier
        assertStandard("$987 == null", Set.of(IGNORED_TYPE));
        assertInternal("$987 == null", Set.of(IGNORED_TYPE));
        assertExternal("$987 == null", unknownType);
    }

    @Test
    void testNotNullLiteral() {
        // not null literal with known field
        assertStandard("!(A == null)", Set.of(IGNORED_TYPE));
        assertInternal("!(A == null)", Set.of(IGNORED_TYPE));
        assertExternal("!(A == null)", aTypes);

        // not null literal with known literal
        assertStandard("!($123 == null)", Set.of(IGNORED_TYPE));
        assertInternal("!($123 == null)", Set.of(IGNORED_TYPE));
        assertExternal("!($123 == null)", identifierType);

        // not null literal with unknown field
        assertStandard("!(D == null)", Set.of(IGNORED_TYPE));
        assertInternal("!(D == null)", Set.of(IGNORED_TYPE));
        assertExternal("!(D == null)", Set.of(UNKNOWN_TYPE));

        // not null literal with unknown identifier
        assertStandard("!($987 == null)", Set.of(IGNORED_TYPE));
        assertInternal("!($987 == null)", Set.of(IGNORED_TYPE));
        assertExternal("!($987 == null)", Set.of(UNKNOWN_TYPE));
    }

    @Test
    void testContentFunctionNoField() {
        assertStandard("content:phrase(termOffsetMap, 'foo', 'bar')", unknownType);
        assertInternal("content:phrase(termOffsetMap, 'foo', 'bar')", unknownType);
        assertExternal("content:phrase(termOffsetMap, 'foo', 'bar')", unknownType);
    }

    @Test
    void testContentFunctionSingleField() {
        // function known field
        assertStandard("content:phrase(A, termOffsetMap, 'foo', 'bar')", aTypes);
        assertInternal("content:phrase(A, termOffsetMap, 'foo', 'bar')", aTypes);
        assertExternal("content:phrase(A, termOffsetMap, 'foo', 'bar')", aTypes);

        // function known identifier
        assertStandard("content:phrase($123, termOffsetMap, 'foo', 'bar')", identifierType);
        assertInternal("content:phrase($123, termOffsetMap, 'foo', 'bar')", identifierType);
        assertExternal("content:phrase($123, termOffsetMap, 'foo', 'bar')", identifierType);

        // function unknown field
        assertStandard("content:phrase(D, termOffsetMap, 'foo', 'bar')", unknownType);
        assertInternal("content:phrase(D, termOffsetMap, 'foo', 'bar')", unknownType);
        assertExternal("content:phrase(D, termOffsetMap, 'foo', 'bar')", unknownType);

        // function unknown identifier
        assertStandard("content:phrase($987, termOffsetMap, 'foo', 'bar')", unknownType);
        assertInternal("content:phrase($987, termOffsetMap, 'foo', 'bar')", unknownType);
        assertExternal("content:phrase($987, termOffsetMap, 'foo', 'bar')", unknownType);
    }

    @Test
    void testContentFunctionMultiField() {
        // function known fields
        assertStandard("content:phrase((A || B), termOffsetMap, 'foo', 'bar')", aTypes);
        assertInternal("content:phrase((A || B), termOffsetMap, 'foo', 'bar')", aTypes);
        assertExternal("content:phrase((A || B), termOffsetMap, 'foo', 'bar')", aTypes);

        // function known identifiers
        assertStandard("content:phrase(($123 || $234), termOffsetMap, 'foo', 'bar')", identifierType);
        assertInternal("content:phrase(($123 || $234), termOffsetMap, 'foo', 'bar')", identifierType);
        assertExternal("content:phrase(($123 || $234), termOffsetMap, 'foo', 'bar')", identifierType);

        // function unknown fields
        assertStandard("content:phrase((D || E), termOffsetMap, 'foo', 'bar')", unknownType);
        assertInternal("content:phrase((D || E), termOffsetMap, 'foo', 'bar')", unknownType);
        assertExternal("content:phrase((D || E), termOffsetMap, 'foo', 'bar')", unknownType);

        // function unknown identifiers
        assertStandard("content:phrase(($987 || $876), termOffsetMap, 'foo', 'bar')", unknownType);
        assertInternal("content:phrase(($987 || $876), termOffsetMap, 'foo', 'bar')", unknownType);
        assertExternal("content:phrase(($987 || $876), termOffsetMap, 'foo', 'bar')", unknownType);

        // TODO -- run through the different combinations

        // known field and known identifier

        // known field and unknown field

        // known identifier and unknown field

        // known identifier and unknown identifier
    }

    // verify fields the correct types
    @Test
    void testSingleTerms() {
        // A field
        assertStandard("A == '1'", aTypes);
        assertInternal("A == '1'", aTypes);
        assertExternal("A == '1'", aTypes);

        // B field
        assertStandard("B == '2'", bTypes);
        assertInternal("B == '2'", bTypes);
        assertExternal("B == '2'", bTypes);

        // C field
        assertStandard("C == '3'", cTypes);
        assertInternal("C == '3'", cTypes);
        assertExternal("C == '3'", cTypes);

        // X field
        assertStandard("X == '7'", xType);
        assertInternal("X == '7'", xType);
        assertExternal("X == '7'", xType);

        // Y field
        assertStandard("Y == '8'", yType);
        assertInternal("Y == '8'", yType);
        assertExternal("Y == '8'", yType);

        // Z field
        assertStandard("Z == '9'", zType);
        assertInternal("Z == '9'", zType);
        assertExternal("Z == '9'", zType);

        // $123 identifier
        assertStandard("$123 == 'a'", identifierType);
        assertInternal("$123 == 'a'", identifierType);
        assertExternal("$123 == 'a'", identifierType);

        // $234 identifier
        assertStandard("$234 == 'b'", identifierType);
        assertInternal("$234 == 'b'", identifierType);
        assertExternal("$234 == 'b'", identifierType);
    }

    @Test
    public void testIdentifierNotNull() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("123", "type-a", LcType.class.getName());

        // identifier
        assertStandard("!($123 == null)", Set.of(IGNORED_TYPE), metadata);
        assertInternal("!($123 == null)", Set.of(IGNORED_TYPE), metadata);
        assertExternal("!($123 >= null)", Set.of("type-a"), metadata);

        // identifier not found, should flip from IGNORED to UNKNOWN
        assertStandard("!($987 == null)", Set.of(IGNORED_TYPE), metadata);
        assertInternal("!($987 == null)", Set.of(IGNORED_TYPE), metadata);
        assertExternal("!($987 >= null)", Set.of(UNKNOWN_TYPE), metadata);
    }

    @Test
    void testSingleton() {
        assertInternal("!(A == '1')", Set.of(IGNORED_TYPE));
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
    void testIntersectionsWithNegatedTerm() {
        // negated terms do not contribute to an intersection
        assertSingleNode("A == '1' && !(B == '2')", aTypes);
        assertSingleNode("B == '2' && !(C == '3')", bTypes);

        // internal visit does not consider negated terms
        assertInternal("A == '1' && !(B == '2')", aTypes);
        assertInternal("B == '2' && !(C == '3')", bTypes);

        // external visit considers negated terms
        assertExternal("A == '1' && !(B == '2')", Sets.intersection(aTypes, bTypes));
        assertExternal("B == '2' && !(C == '3')", Set.of());
    }

    @Test
    void testIntersectionsWithNullTerm() {
        // negated terms do not contribute to an intersection
        assertSingleNode("A == '1' && !(B == null)", aTypes);
        assertSingleNode("B == '2' && !(C == null)", bTypes);

        // internal visit does not consider negated terms
        assertInternal("A == '1' && !(B == null)", aTypes);
        assertInternal("B == '2' && !(C == null)", bTypes);

        // external visit considers negated terms
        assertExternal("A == '1' && !(B == null)", Sets.intersection(aTypes, bTypes));
        assertExternal("B == '2' && !(C == null)", Set.of());
    }

    @Test
    void testUnknownType() {
        assertSingleNode("A == '1' && D == '4'", Collections.singleton(IngestTypeVisitor.UNKNOWN_TYPE));
        assertSingleNode("A == '1' || D == '4'", Collections.singleton(IngestTypeVisitor.UNKNOWN_TYPE));
        assertSingleNode("D == '4' && E == '5'", Collections.singleton(IngestTypeVisitor.UNKNOWN_TYPE));
        assertSingleNode("D == '4' || E == '5'", Collections.singleton(IngestTypeVisitor.UNKNOWN_TYPE));
    }

    @Test
    void testEqNull() {
        assertSingleNode("A == null", Set.of(IGNORED_TYPE));
        assertSingleNode("!(A == null)", Set.of(IGNORED_TYPE));
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

    @Test
    void testFilterFunctionExcludeExpandedIntoMutuallyExclusiveFields() {
        // there might be an exclude like #EXCLUDE(MODEL_FIELD, '.*.*')
        // which is expanded like so #EXCLUDE((F1||F2||F3), '.*.*')
        // and is then rewritten as a filter function like so !((F1 == null && F2 == null && F3 == null))
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("C", "type2", LcType.class.getTypeName());
        metadata.put("D", "type3", LcType.class.getTypeName());

        String query = "A == '1' && !((B == null || C == null || D == null))";
        test(query, Collections.singleton("type1"), metadata);
    }

    @Test
    void testUnionOfNegatedTerms() {
        String query = "!(A == '1') || !(B == '2') || !(C == '3')";
        test(query, Set.of(IGNORED_TYPE));
    }

    @Test
    void testUnionOfNotNullTerms() {
        String query = "!(A == null) || !(B == null) || !(C == null)";
        test(query, Set.of(IGNORED_TYPE));
    }

    @Test
    void testIntersectionEdgeCases() {
        // all null literals
        test("A == null && B == null", Set.of(IGNORED_TYPE));
        // all negated
        test("!(A == '1') && !(B == '2')", Set.of(IGNORED_TYPE));
        // mix of negated terms and null literals
        test("!(A == '1') && B == null", Set.of(IGNORED_TYPE));

        // anchor term and negated term
        test("A == '1' && !(B == '2')", aTypes);
        test("!(B == '2') && A == '1'", aTypes);
        // anchor term and null literal
        test("A == '1' && B == null", aTypes);
        test("B == null && A == '1' ", aTypes);
        // anchor term and negated null literal
        test("A == '1' && !(B == null)", aTypes);
        test("!(B == null) && A == '1'", aTypes);
    }

    //  @formatter:off
    //  proposed rules
    //  1. leaf nodes can return NO_TYPE or UNKNOWN_TYPE
    //  2. intersection/union logic knows how to return this
    //  3. pruning logic knows how to ignore it
    //  4. will only prune exclusive, single node negations [A == '1' && !(B == '2')]

    //  pruning needs to know if it's internal or external pruning.
    //  1. internal pruning rules are different vs. external when it comes to intersecting negated/null terms
    //  for example; A == '1' && !(B == null)
    //  in isolation this is not executable and should prune. but if the context is
    //  this: (B == '0' && (Z == '9' || (A == '1' && !(B == null))...then who knows
    //  @formatter:on

    @Test
    void testUnionEdgeCases() {
        // all null literals
        test("A == null || B == null", Set.of(IGNORED_TYPE));
        // all negated
        test("!(A == '1') || !(B == '2')", Set.of(IGNORED_TYPE));
        // mix of negated terms and null literals
        test("!(A == '1') || B == null", Set.of(IGNORED_TYPE));

        // anchor term and negated term
        test("A == '1' || !(B == '2')", aTypes);
        test("!(B == '2') || A == '1'", aTypes);
        // anchor term and null literal
        test("A == '1' || B == null", aTypes);
        test("B == null || A == '1'", aTypes);
        // anchor term and negated null literal
        test("A == '1' || !(B == null)", aTypes);
        test("!(B == null) || A == '1'", aTypes);
    }

    @Test
    void testRandomUnionOfSimpleTerms() throws Exception {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("E", "type5", LcType.class.getTypeName());
        metadata.put("F", "type6", LcType.class.getTypeName());

        Set<String> fields = Set.of("A", "B", "C", "D", "E", "F");
        Set<String> values = Set.of("v1", "v2", "v3", "v4", "v5");

        JexlQueryGenerator generator = new JexlQueryGenerator(fields, values);
        generator.disableAllOptions();
        generator.disableIntersections();
        generator.enableUnions();

        int minTerms = 2;
        int maxTerms = 10;
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            String query = generator.getQuery(minTerms, maxTerms);
            JexlNode node = JexlASTHelper.parseAndFlattenJexlQuery(query);
            Set<String> types = IngestTypeVisitor.getIngestTypes(node, metadata);

            Set<String> queryFields = JexlASTHelper.getIdentifierNames(node);
            Set<String> queryTypes = new HashSet<>();
            for (String queryField : queryFields) {
                Set<String> typesForField = metadata.getDataTypesForField(queryField);
                if (typesForField != null) {
                    queryTypes.addAll(typesForField);
                }
            }

            if (types.isEmpty() || types.size() != queryTypes.size()) {
                Set<String> typesForTesting = IngestTypeVisitor.getIngestTypes(node, metadata);
                int k = 0;
            }
            assertEquals(queryTypes, types);
        }
    }

    @Test
    void testRandomIntersectionOfSimpleTerms() throws Exception {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("A", "type2", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("B", "type3", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("C", "type4", LcType.class.getTypeName());

        Set<String> fields = Set.of("A", "B", "C");
        Set<String> values = Set.of("v1", "v2", "v3", "v4", "v5");

        JexlQueryGenerator generator = new JexlQueryGenerator(fields, values);
        generator.disableAllOptions();
        generator.disableUnions();
        generator.enableIntersections();

        int minTerms = 2;
        int maxTerms = 10;
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            String query = generator.getQuery(minTerms, maxTerms);
            JexlNode node = JexlASTHelper.parseAndFlattenJexlQuery(query);
            Set<String> types = IngestTypeVisitor.getIngestTypes(node, metadata);

            Set<String> queryFields = JexlASTHelper.getIdentifierNames(node);
            Set<String> queryTypes = new HashSet<>();
            for (String queryField : queryFields) {
                Set<String> typesForField = metadata.getDataTypesForField(queryField);

                if (queryTypes.isEmpty()) {
                    queryTypes.addAll(typesForField);
                } else {
                    queryTypes.retainAll(typesForField);
                    if (queryTypes.isEmpty()) {
                        break;
                    }
                }
            }

            if (types.size() != queryTypes.size()) {
                Set<String> typesForTesting = IngestTypeVisitor.getIngestTypes(node, metadata);
                int k = 0;
            }
            assertEquals(queryTypes, types);
        }
    }

    private void assertSingleNode(String query, Set<String> expectedIngestTypes) {
        assertSingleNode(query, expectedIngestTypes, typeMetadata);
    }

    private void assertSingleNode(String query, Set<String> expectedIngestTypes, TypeMetadata typeMetadata) {
        JexlNode node = parseQuery(query);
        Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(node, typeMetadata);
        assertEquals(expectedIngestTypes, ingestTypes);
    }

    private void test(String query, Set<String> expected) {
        assertStandard(query, expected);
    }

    private void test(String query, Set<String> expected, TypeMetadata typeMetadata) {
        assertStandard(query, expected, typeMetadata);
    }

    /**
     * Standard entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, TypeMetadata)}
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     */
    private void assertStandard(String query, Set<String> expected) {
        assertStandard(query, expected, typeMetadata);
    }

    /**
     * Standard entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, TypeMetadata)}
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     * @param metadata
     *            the {@link TypeMetadata}
     */
    private void assertStandard(String query, Set<String> expected, TypeMetadata metadata) {
        ASTJexlScript script = parseQuery(query);
        Set<String> types = IngestTypeVisitor.getIngestTypes(script, metadata);
        assertEquals(new TreeSet<>(expected), new TreeSet<>(types)); // sorted sets make differences easy to spot
    }

    /**
     * External entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, boolean)} with boolean set to true
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     */
    private void assertExternal(String query, Set<String> expected) {
        assertExternal(query, expected, typeMetadata);
    }

    /**
     * External entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, boolean)} with boolean set to true
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     * @param metadata
     *            the {@link TypeMetadata}
     */
    private void assertExternal(String query, Set<String> expected, TypeMetadata metadata) {
        assertTypes(query, expected, metadata, true);
    }

    /**
     * Internal entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, boolean)} with boolean set to false
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     */
    private void assertInternal(String query, Set<String> expected) {
        assertInternal(query, expected, typeMetadata);
    }

    /**
     * Internal entry via {@link IngestTypeVisitor#getIngestTypes(JexlNode, boolean)} with boolean set to false
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     * @param metadata
     *            the {@link TypeMetadata}
     */
    private void assertInternal(String query, Set<String> expected, TypeMetadata metadata) {
        assertTypes(query, expected, metadata, false);
    }

    /**
     * Assert types for the reusable entry point into the {@link IngestTypeVisitor} using the external argument.
     *
     * @param query
     *            the query
     * @param expected
     *            the expected types
     * @param metadata
     *            the {@link TypeMetadata}
     * @param external
     *            boolean flag indicating what type of visit this is
     */
    private void assertTypes(String query, Set<String> expected, TypeMetadata metadata, boolean external) {
        ASTJexlScript script = parseQuery(query);
        IngestTypeVisitor visitor = new IngestTypeVisitor(metadata);
        Set<String> types = visitor.getIngestTypes(script.jjtGetChild(0), external);
        assertEquals(new TreeSet<>(expected), new TreeSet<>(types)); // sorted sets make differences easy to spot
    }

    /**
     * Helper routine to parse a query, or fail the test with a helpful message
     *
     * @param query
     *            the query
     * @return an ASTJexlScript
     */
    private ASTJexlScript parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException("Failed to parse query");
        }
    }
}
