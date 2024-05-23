package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.core.query.jexl.visitors.validate.ASTValidator;
import datawave.data.type.LcType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;

public class IngestTypePruningVisitorTest {

    private static final Logger log = Logger.getLogger(IngestTypePruningVisitorTest.class);

    private static final TypeMetadata typeMetadata = new TypeMetadata();
    private final ASTValidator validator = new ASTValidator();

    @BeforeClass
    public static void setup() {
        typeMetadata.put("A", "ingestType1", LcType.class.getTypeName());
        typeMetadata.put("A", "ingestType2", LcType.class.getTypeName());
        typeMetadata.put("A", "ingestType3", LcType.class.getTypeName());

        typeMetadata.put("B", "ingestType1", LcType.class.getTypeName());
        typeMetadata.put("B", "ingestType2", LcType.class.getTypeName());

        typeMetadata.put("C", "ingestType5", LcType.class.getTypeName());

        typeMetadata.put("123", "ingestType1", LcType.class.getTypeName());
    }

    @Test
    public void testNoOps() {
        //  @formatter:off
        String[] queries = {
                        "A == '1' || B == '2'",
                        "A == '1' && B == '2'"
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, query);
        }
    }

    // test cases for no pruning, multiple node types
    @Test
    public void testNoOpsWithMultipleLeafTypes() {
        //  @formatter:off
        String[] queries = {
                        "A == '1' && B == '2'",
                        "A == '1' && B != '2'",
                        "A == '1' && !(B == '2')",
                        "A == '1' && B =~ '2'",
                        "A == '1' && B !~ '2'",
                        "A == '1' && !(B =~ '2')",
                        "A == '1' && B < '2'",
                        "A == '1' && B <= '2'",
                        "A == '1' && B > '2'",
                        "A == '1' && B >= '2'",
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, query);
        }
    }

    // case where two nodes do not share an ingest type
    @Test
    public void testEmptyIntersection() {
        //  @formatter:off
        String[] queries = {
                        "A == '1' && C == '3'",
                        "A == '1' && B == '2' && C == '3'",
                        "A == '1' && C != '3'",
                        "A == '1' && C =~ '3'",
                        "A == '1' && C !~ '3'",
                        "A == '1' && C < '3'",
                        "A == '1' && C <= '3'",
                        "A == '1' && C > '3'",
                        "A == '1' && C >= '3'",
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, null);
        }
    }

    @Test
    public void testIntersectionWithExclusiveNegation() {
        test("A == '1' && !(C == '3')", "A == '1'");
        test("A == '1' && !(C =~ '3')", "A == '1'");
    }

    // A && (B || C)
    // ingestType 1 = A, B
    // ingestType 2 = C
    @Test
    public void testPruneNestedUnion() {
        // prune C term
        String query = "A == '1' && (B == '2' || C == '3')";
        String expected = "A == '1' && B == '2'";
        test(query, expected);

        // prune multiple C terms
        query = "A == '1' && (B == '2' || C == '3' || C == '4')";
        expected = "A == '1' && B == '2'";
        test(query, expected);

        // whole union pruned, which leads to whole query getting pruned
        query = "A == '1' && (C == '3' || C == '4')";
        test(query, null);
    }

    // A && (B || C)
    // ingestType 1 = A, B
    // ingestType 2 = C
    @Test
    public void testPruneComplexNestedUnion() {
        // double nested C term pruned
        String query = "A == '1' && (B == '2' || (C == '3' && C == '5'))";
        String expected = "A == '1' && B == '2'";
        test(query, expected);

        // double nested C term pruned, nested union persists
        query = "A == '1' && (B == '2' || B == '0' || (C == '3' && C == '5'))";
        expected = "A == '1' && (B == '2' || B == '0')";
        test(query, expected);

        // double nested intersection of A and C pruned, nested union persists
        query = "A == '1' && (B == '2' || B == '0' || (C == '3' && A == '15'))";
        expected = "A == '1' && (B == '2' || B == '0')";
        test(query, expected);
    }

    @Test
    public void testOtherComplexNestedUnion() {
        // doesn't matter how complex the nesting is, C term should drive pruning
        String query = "C == '1' && (B == '2' || B == '3' || (A == '4' && A == '5'))";
        test(query, null);
    }

    @Test
    public void testDoubleNestedPruning() {
        // base case, should be fine
        String query = "(A == '1' || B == '2') && (A == '3' || B == '4')";
        test(query, query);

        // no intersection of types
        query = "(A == '1' || B == '2') && (C == '3' || C == '4')";
        test(query, null);

        // no intersection of types
        query = "(C == '1' || C == '2') && (A == '3' || B == '4')";
        test(query, null);
    }

    @Test
    public void testDoubleNestedUnionWithRangeStreamPruning() {
        // this case demonstrates how a top level query could pass ingest type pruning
        // but still get modified by range stream pruning. In some cases further pruning
        // by this visitor would be necessary.

        // query passes ingest type pruning without issue
        String query = "(A == '1' || C == '2') && (B == '3' || C == '4')";
        test(query, query);

        // A term pruned by range stream, B term has no effect on resulting query
        query = "C == '2' && (B == '3' || C == '4')";
        test(query, "C == '2' && C == '4'");

        // B term pruned by range stream, C term has no effect on resulting query
        query = "(A == '1' || C == '2') && B == '3'";
        test(query, "A == '1' && B == '3'");

        // left C term pruned by range stream, right C term has no effect on resulting query
        query = "A == '1' && (B == '3' || C == '4')";
        test(query, "A == '1' && B == '3'");

        // right C term pruned by range stream, left C term has no effect on resulting query
        query = "(A == '1' || C == '2') && B == '3'";
        test(query, "A == '1' && B == '3'");

        // left union pruned by range stream, no pruning to do in resulting query
        query = "B == '3' || C == '4'";
        test(query, query);

        // right union pruned by range stream, no pruning to do in resulting query
        query = "A == '1' || C == '2'";
        test(query, query);
    }

    @Test
    public void testOverlappingExclusions() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("A", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType3", LcType.class.getTypeName());
        metadata.put("C", "ingestType3", LcType.class.getTypeName());
        metadata.put("C", "ingestType4", LcType.class.getTypeName());
        metadata.put("D", "ingestType4", LcType.class.getTypeName());
        metadata.put("D", "ingestType5", LcType.class.getTypeName());

        // A && B prune to ingestType 2
        // C && D prune to ingestType 4
        // top level B term intersects with union of ingest types 2, 4 producing a singleton of ingestType 2
        // range stream pruning means we could still end up with a non-viable query
        // if the A term is not found
        String query = "B == '22' && ((A == '1' && B == '2') || (C == '3' && D == '4'))";
        String expected = "B == '22' && (A == '1' && B == '2')";
        test(query, expected, metadata);
    }

    @Test
    public void testYetAnotherComplexNestedUnion() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType1", LcType.class.getTypeName());
        metadata.put("C", "ingestType2", LcType.class.getTypeName());
        metadata.put("D", "ingestType2", LcType.class.getTypeName());

        // need the complex case when a top level intersection drives the pruning of a nested union-intersection
        // A && (B || (C && D))
        // A = 1
        // B = 1
        // C = 2
        // D = 2

        String query = "A == '1' && (B == '2' || (C == '3' && D == '4'))";
        String expected = "A == '1' && B == '2'";
        test(query, expected, metadata);

        // same datatypes, drop the single union term
        query = "A == '1' && (C == '5' || (A == '2' && B == '3'))";
        expected = "A == '1' && A == '2' && B == '3'";
        test(query, expected, metadata);

        query = "C == '1' && (A == '2' || (B == '3' && C == '4'))";
        test(query, null);
    }

    @Test
    public void testIntersectionsWithNonIndexedFields() {
        //  @formatter:off
        String[] queries = {
                        //  D term is not indexed
                        "A == '1' && D == '3'",
                        "A == '1' && B == '2' && D == '3'",
                        "A == '1' && D != '3'",
                        "A == '1' && !(D == '3')",
                        "A == '1' && D =~ '3'",
                        "A == '1' && D !~ '3'",
                        "A == '1' && !(D =~ '3')"
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, query);
        }
    }

    @Test
    public void testIntersectionsWithIncompleteUnions() {
        //  @formatter:off
        String[] queries = {
                        "A == '1' && (B == 2 || filter:includeRegex(D, 'value.*'))",
                        "A == '1' && (B == 2 || filter:excludeRegex(D, 'value.*'))",
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, query);
        }
    }

    @Test
    public void testIntersectionsWithQueryFunctions() {
        // each function type

        //  @formatter:off
        String[] queries = {
                        "A == '1' && f:between(B, a, b)",
                        "A == '1' && f:length(B, '2', '3')",
                        //  by the time the ingestType pruning visitor is run, a multi-fielded
                        //  include function should be decomposed into discrete functions
                        "A == '1' && f:includeText(B, 'ba.*')",
                        "A == '1' && f:matchRegex(B, 'ba.*')",
                        "A == '1' && f:matchRegex(B, C, 'ba.*')",
        };
        //  @formatter:on

        // no change for these queries
        for (String query : queries) {
            test(query, query);
        }
    }

    @Test
    public void testIntersectionsWithMarkers() {
        // all marker node types
        //  @formatter:off
        String[] queries = {
                        "A == '1' && ((_Bounded_ = true) && (B >= '0' && B <= '10'))",
                        "A == '1' && ((_Delayed_ = true) && (B == '2'))",
                        "A == '1' && ((_Delayed_ = true) && (A == '1' || B == '2'))",
                        "A == '1' && ((_Delayed_ = true) && (A == '1' && B == '2'))",
                        "A == '1' && ((_Eval_ = true) && (B == '2'))",
                        "A == '1' && ((_List_ = true) && ((id = 'some-bogus-id') && (field = 'B') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))",
                        "A == '1' && ((_Term_ = true) && (B == '2'))",
                        "A == '1' && ((_Value_ = true) && (B =~ 'ba.*'))",
                        "A == '1' && ((_Value_ = true) && (A =~ 'ab.*' || B =~ 'ba.*'))",
                        "A == '1' && ((_Value_ = true) && (A =~ 'ab.*' && B =~ 'ba.*'))"
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, query);
        }

        // same queries as above, test pruning
        //  @formatter:off
        queries = new String[] {
                        "A == '1' && ((_Bounded_ = true) && (C >= '0' && C <= '10'))",
                        "A == '1' && ((_Delayed_ = true) && (C == '2'))",
                        "A == '1' && ((_Eval_ = true) && (C == '2'))",
                        "A == '1' && ((_List_ = true) && ((id = 'some-bogus-id') && (field = 'C') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))",
                        "A == '1' && ((_Term_ = true) && (C == '2'))",
                        "A == '1' && ((_Value_ = true) && (C =~ 'ba.*'))"
        };
        //  @formatter:on

        for (String query : queries) {
            test(query, null);
        }
    }

    @Test
    public void testMultiFieldedMarkers() {
        // case 1: delayed intersection of non-intersecting ingestTypes should remove itself
        String query = "((_Delayed_ = true) && (A == '1' && C == '2'))";
        test(query, null);

        // case 2: overlapping ingestTypes
        query = "A == '1' && ((_Delayed_ = true) && (B == '1' || C == '2'))";
        test(query, "A == '1' && ((_Delayed_ = true) && (B == '1'))");

        // case 3: non-intersecting ingestTypes (function removes itself)
        query = "A == '1' && ((_Delayed_ = true) && (A == '1' && C == '2'))";
        test(query, null);

        // case 4: unknown field and how that works
        query = "((_Delayed_ = true) && (A == '1' && D == '2'))";
        test(query, query);
    }

    @Test
    public void testDelayedBoundedMarker() {
        String query = "((_Delayed_ = true) && ((_Bounded_ = true) && (A > '2' && A < '4')))";
        test(query, query);

        // C term drives pruning of double nested marker
        query = "C == '1' && ((_Delayed_ = true) && ((_Bounded_ = true) && (A > '2' && A < '4')))";
        test(query, null);

        query = "((_Delayed_ = true) && ((_Bounded_ = true) && (A > '2' && A < '4'))) && C == '1'";
        test(query, null);
    }

    @Test
    public void testDelayedEvaluationOnlyMarker() {
        String query = "((_Delayed_ = true) && ((_Eval_ = true) && (A == '1')))";
        test(query, query);

        // C term drives pruning of double nested marker
        query = "C == '1' && ((_Delayed_ = true) && ((_Eval_ = true) && (A == '1')))";
        test(query, null);
    }

    @Test
    public void testDelayedListMarker() {
        String query = "((_Delayed_ = true) && ((_List_ = true) && ((id = 'some-bogus-id') && (field = 'A') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))";
        test(query, query);

        // C term drives pruning of double nested marker
        query = "C == '1' && ((_Delayed_ = true) && ((_List_ = true) && ((id = 'some-bogus-id') && (field = 'A') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))";
        test(query, null);
    }

    @Test
    public void testDelayedTermMarker() {
        String query = "((_Delayed_ = true) && ((_Term_ = true) && (A =~ 'ba.*')))";
        test(query, query);

        // C term drives pruning of double nested marker
        query = "C == '1' && ((_Delayed_ = true) && ((_Term_ = true) && (A =~ 'ba.*')))";
        test(query, null);
    }

    @Test
    public void testDelayedValueMarker() {
        String query = "((_Delayed_ = true) && ((_Value_ = true) && (A =~ 'ba.*' && B =~ 'ba.*')))";
        test(query, query);

        // C term drives pruning of double nested markers
        query = "C == '1'  && ((_Delayed_ = true) && ((_Value_ = true) && (A =~ 'ba.*' && B =~ 'ba.*')))";
        test(query, null);

        // root marker with multiple conflicting sources should self-prune
        query = "((_Delayed_ = true) && ((_Value_ = true) && (A =~ 'ba.*' && C =~ 'ba.*')))";
        test(query, null);
    }

    @Test
    public void testMultiFieldedFunctions() {
        String query = "A == '1' && filter:compare(A,'==','ANY','C')";
        test(query, query);
    }

    @Test
    public void testEvaluationOnlyField() {
        // evaluation only fields are not guaranteed to have an 'e' column in
        // the datawave metadata table. In this case the Z term has no entry.
        String query = "A == '1' && Z == '2'";
        test(query, query);
    }

    @Test
    public void testPruneNegation() {
        // internal prune
        String query = "A == '1' || !((_Delayed_ = true) && (A == '2' && C == '3'))";
        test(query, "A == '1'");

        query = "A == '0' && (A == '1' || !((_Delayed_ = true) && (A == '2' && C == '3')))";
        test(query, "A == '0' && (A == '1')");
    }

    @Test
    public void testFullyPrunedTree() {
        String query = "(false)";
        test(query, query);
    }

    @Test
    public void testIdentifiers() {
        String query = "A == '1' && $123 == '123'";
        test(query, query);

        query = "C == '1' && $123 == '123'";
        test(query, null);
    }

    @Test
    public void testArithmetic() {
        String query = "A == '1' && 1 + 1 == 3";
        test(query, query);
    }

    @Test
    public void testPruneNestedMarker() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("A", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType3", LcType.class.getTypeName());
        metadata.put("B", "ingestType4", LcType.class.getTypeName());
        metadata.put("C", "ingestType3", LcType.class.getTypeName());
        metadata.put("C", "ingestType4", LcType.class.getTypeName());

        String query = "A == '1' && (((_Delayed_ = true) && (B =~ 'b.*')) || ((_Delayed_ = true) && (C =~ 'c.*')))";
        String expected = "A == '1' && (((_Delayed_ = true) && (B =~ 'b.*')))";
        test(query, expected, typeMetadata);
    }

    @Test
    public void testExternalPrune() {
        testExternalPrune("A == '1' || B == '2'", null, Collections.singleton("ingestType5"));
        testExternalPrune("A == '1' && B == '2'", null, Collections.singleton("ingestType5"));

        // and with our own type metadata
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType1", LcType.class.getTypeName());
        metadata.put("C", "ingestType1", LcType.class.getTypeName());

        test("A == '1' && (B == '2' || C == '3')", null, metadata, Collections.singleton("ingestType2"));
        test("A == '1' || (B == '2' && C == '3')", null, metadata, Collections.singleton("ingestType2"));
    }

    @Test
    public void testExternalPruneWithSelfPrune() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("C", "ingestType3", LcType.class.getTypeName());
        metadata.put("D", "ingestType4", LcType.class.getTypeName());

        String query = "A == '1' || B == '2' || (C == '3' && D == '4')";
        String expected = "B == '2'";

        Set<String> externalTypes = Sets.newHashSet("ingestType2", "ingestType3", "ingestType4");
        // A term pruned by external types
        // C and D terms should self prune
        test(query, expected, metadata, externalTypes);
    }

    @Test
    public void testAndNull() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("9", "ingestType2", LcType.class.getTypeName());

        // *technically* valid because B will always be null for any document that matches A
        // practically the B term is superfluous
        String query = "A == '1' && B == null";
        test(query, "A == '1'", metadata);

        query = "A == '1' && $9 == null"; // same form but with an identifier
        test(query, "A == '1'", metadata);
    }

    @Test
    public void testAndNotNull() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType1", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("9", "ingestType2", LcType.class.getTypeName());

        // visitor is smart enough to prune out a negation for an exclusive ingest type
        String query = "A == '1' && !(B == null)";
        test(query, "A == '1'", metadata);

        query = "A == '1' && !($B == null)"; // same form but with an identifier
        test(query, "A == '1'", metadata);
    }

    @Test
    public void testSpecificCase() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType2", LcType.class.getTypeName());
        metadata.put("B", "ingestType3", LcType.class.getTypeName());
        metadata.put("B", "ingestType4", LcType.class.getTypeName());
        metadata.put("C", "ingestType1", LcType.class.getTypeName());
        metadata.put("C", "ingestType2", LcType.class.getTypeName());
        metadata.put("C", "ingestType3", LcType.class.getTypeName());
        metadata.put("C", "ingestType4", LcType.class.getTypeName());
        metadata.put("D", "ingestType3", LcType.class.getTypeName());
        metadata.put("D", "ingestType4", LcType.class.getTypeName());
        metadata.put("E", "ingestType1", LcType.class.getTypeName());
        metadata.put("E", "ingestType2", LcType.class.getTypeName());
        metadata.put("F", "ingestType3", LcType.class.getTypeName());
        metadata.put("F", "ingestType4", LcType.class.getTypeName());
        metadata.put("9", "ingestType1", LcType.class.getTypeName());
        metadata.put("9", "ingestType2", LcType.class.getTypeName());
        metadata.put("9", "ingestType3", LcType.class.getTypeName());
        metadata.put("9", "ingestType4", LcType.class.getTypeName());

        String query = "(A == '1' || B == '2') && C == '3' && D == null && $9 == null && !(E == '4') && !(E == '5' || E == '6') && !(F == '7')";
        test(query, query, metadata);
    }

    @Test
    public void testNotNullAndNestedUnion() {
        String query = "!(A == null) && B == '1' || ((C == '2' || D == '2' || E == '2' || F == '2'))";

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

        test(query, query, metadata);
    }

    @Test
    public void testFilterFunctionExcludeExpandedIntoMutuallyExclusiveFields() {
        // there might be an exclude like #EXCLUDE(MODEL_FIELD, '.*.*')
        // which is expanded like so #EXCLUDE((F1||F2||F3), '.*.*')
        // and is then rewritten as a filter function like so !((F1 == null && F2 == null && F3 == null))
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("C", "type2", LcType.class.getTypeName());
        metadata.put("D", "type3", LcType.class.getTypeName());

        String query = "A == '1' && !((B == null || C == null || D == null))";
        String expected = "A == '1' && !((B == null))";
        test(query, expected, metadata);
    }

    @Test
    public void testUnionOfNegatedTerms() {
        String query = "!(A == '1') || !(B == '2') || !(C == '3')";
        test(query, query);
    }

    @Test
    public void testUnionOfNotNullTerms() {
        String query = "!(A == null) || !(B == null) || !(C == null)";
        test(query, query);
    }

    private void test(String query, String expected) {
        test(query, expected, typeMetadata, null);
    }

    private void testExternalPrune(String query, String expected, Set<String> ingestTypes) {
        test(query, expected, typeMetadata, ingestTypes);
    }

    private void test(String query, String expected, TypeMetadata metadata) {
        test(query, expected, metadata, null);
    }

    private void test(String query, String expected, TypeMetadata metadata, Set<String> ingestTypes) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript pruned = (ASTJexlScript) IngestTypePruningVisitor.prune(script, metadata, ingestTypes);

            log.info("input   : " + query);
            log.info("output  : " + JexlStringBuildingVisitor.buildQuery(pruned));
            log.info("expected: " + expected);

            // all pruned scripts must be valid
            assertTrue(validator.isValid(pruned));

            // we might be expecting nothing as a result
            if (expected == null) {
                log.trace("expected null! " + JexlStringBuildingVisitor.buildQuery(pruned));
                assertEquals("failed for query: " + query, 0, pruned.jjtGetNumChildren());
                return;
            }

            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
            TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, pruned);
            assertTrue("Jexl tree comparison failed with reason: " + comparison.getReason(), comparison.isEqual());

        } catch (Exception e) {
            e.printStackTrace();
            fail("test failed: " + e.getMessage());
        }
    }
}
