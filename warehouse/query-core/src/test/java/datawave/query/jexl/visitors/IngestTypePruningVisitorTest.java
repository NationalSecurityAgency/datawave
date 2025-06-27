package datawave.query.jexl.visitors;

import static datawave.common.test.utils.query.RangeFactoryForTests.makeShardedRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTestRange;
import static datawave.query.jexl.visitors.IngestTypeVisitor.IGNORED_TYPE;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.ingest.protobuf.Uid;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.index.lookup.RangeStream;
import datawave.query.jexl.util.JexlQueryGenerator;
import datawave.query.model.QueryModel;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.RandomTreeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.data.type.LcType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.util.TypeMetadata;

import javax.validation.constraints.AssertFalse;

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

    @Before
    public void beforeEach() {
        validator.enableAll();
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
        test(query, "A == '0' && A == '1'");
    }

    @Test
    public void testFullyPrunedTree() {
        String query = "(false)";
        test(query, "");
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
        String expected = "A == '1' && ((_Delayed_ = true) && (B =~ 'b.*'))";
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

        testExternalPrune("A == '1' && (B == '2' || C == '3')", null, metadata, Collections.singleton("ingestType2"));
        testExternalPrune("A == '1' || (B == '2' && C == '3')", null, metadata, Collections.singleton("ingestType2"));
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
        testExternalPrune(query, expected, metadata, externalTypes);
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

        // not null exclusive type evaluates to false, causing whole intersection to be dropped
        String query = "A == '1' && !(B == null)";
        test(query, "", metadata);

        query = "A == '1' && !($B == null)"; // same form but with an identifier
        test(query, "", metadata);
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

    @Ignore
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

        // pushdown negations visitor would rewrite this
        // into A == '1' && !(B == null) && !...
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

    @Test
    public void testIntersectionsWithNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, no change
        String query = "A == '1' && A == null";
        test(query, query, metadata);

        // different field, same type, no change
        query = "A == '1' && B == null";
        test(query, query, metadata);

        // field with exclusive type, pruned
        query = "A == '1' && C == null";
        test(query, "A == '1'", metadata);

        // identifier with same type, no change
        query = "A == '1' && $123 == null";
        test(query, query, metadata);

        // identifier with exclusive type, prune
        query = "A == '1' && $234 == null";
        test(query, "A == '1'", metadata);
    }

    // is not null terms with exclusive datatypes should NOT be pruned
    // the query is *technically* non-executable
    @Test
    public void testIntersectionsWithNotNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, no change
        String query = "A == '1' && !(A == null)";
        test(query, query, metadata);

        // different field, same type, no change
        query = "A == '1' && !(B == null)";
        test(query, query, metadata);

        // not null term with exclusive type is false, prune whole intersection
        query = "A == '1' && !(C == null)";
        test(query, "", metadata);

        // identifier with same type, no change
        query = "A == '1' && !($123 == null)";
        test(query, query, metadata);

        // identifier with exclusive type, prune
        query = "A == '1' && !($234 == null)";
        test(query, "", metadata);
    }

    @Test
    public void testNestedUnionsWithNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, same types, no change
        String query = "A == '1' && (A == '2' || A == null)";
        test(query, query, metadata);

        // different fields, subset types, no change
        query = "A == '1' && (A == '2' || B == null)";
        test(query, query, metadata);

        // exclusive null term evaluates to true, should prune the whole union
        query = "A == '1' && (B == '2' || C == null)";
        String expected = "A == '1'";
        test(query, expected, metadata);
    }

    @Test
    public void testNestedUnionsWithNotNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        String query = "A == '1' && (A == '2' || !(A == null))";
        test(query, query, metadata);

        query = "A == '1' && (A == '2' || !(B == null))";
        test(query, query, metadata);

        // not null exclusive term evaluates to false, can safely drop from a union
        query = "A == '1' && (B == '2' || !(C == null))";
        test(query, "A == '1' && B == '2'", metadata);
    }

    @Test
    public void testNestedUnionsWithNegatedTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, same types, no change
        String query = "A == '1' && (A == '2' || !(A == '3'))";
        test(query, query, metadata);

        // different fields, subset types, no change
        query = "A == '1' && (A == '2' || !(B == '3'))";
        test(query, query, metadata);

        // exclusive negated term evaluates to true, drop the whole union
        query = "A == '1' && (B == '2' || !(C == '3'))";
        String expected = "A == '1'";
        test(query, expected, metadata);
    }

    @Test
    public void testNestedIntersectionsWithNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, same types, no change
        String query = "A == '1' || (A == '2' && A == null)";
        test(query, query, metadata);

        // different fields, subset types, no change
        query = "A == '1' || (A == '2' && B == null)";
        test(query, query, metadata);

        // exclusive type evaluates to true, may be safely pruned
        query = "A == '1' || (A == '2' && C == null)";
        test(query, "A == '1' || A == '2'", metadata);
    }

    @Test
    public void testNestedIntersectionsWithNotNullTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        String query = "A == '1' || (A == '2' && !(A == null))";
        test(query, query, metadata);

        query = "A == '1' || (A == '2' && !(B == null))";
        test(query, query, metadata);

        // not null term for exclusive type evaluates to false, whole intersection must be pruned
        query = "A == '1' || (A == '2' && !(C == null))";
        test(query, "A == '1'", metadata);
    }

    @Test
    public void testNestedIntersectionsWithNegatedTerms() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-2", LcType.class.getTypeName());
        metadata.put("123", "type-1", LcType.class.getTypeName());
        metadata.put("234", "type-2", LcType.class.getTypeName());

        // same field, same types, no change
        String query = "A == '1' || (A == '2' && !(A == '3'))";
        test(query, query, metadata);

        // different fields, subset types, no change
        query = "A == '1' || (A == '2' && !(B == '3'))";
        test(query, query, metadata);

        // negated term for exclusive type evaluates to false, safely dropped from intersection
        query = "A == '1' || (A == '2' && !(C == '3'))";
        test(query, "A == '1' || A == '2'", metadata);
    }

    /**
     * This query is technically a top level union, but the nested intersections are <b>both</b> top level intersections and can thus drive pruning in their
     * nested unions.
     */
    @Test
    public void testContrivedCaseWithTwoTopLevelIntersectionsUnderUnion() {

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type-1", LcType.class.getTypeName());
        metadata.put("B", "type-1", LcType.class.getTypeName());
        metadata.put("C", "type-3", LcType.class.getTypeName());
        metadata.put("X", "type-7", LcType.class.getTypeName());
        metadata.put("Y", "type-7", LcType.class.getTypeName());
        metadata.put("Z", "type-9", LcType.class.getTypeName());

        String query = "(A == '1' && (B == '2' || C == '3')) || (X == '7' && (Y == '8' || Z == '9'))";
        String expected = "(A == '1' && B == '2') || (X == '7' && Y == '8')";
        test(query, expected, metadata);
    }

    private void test(String query, String expected) {
        test(query, expected, typeMetadata);
    }

    private void test(String query, String expected, TypeMetadata metadata) {
        ASTJexlScript internal = testInternalPrune(query, expected, metadata);
        ASTJexlScript external = testExternalPrune(query, expected, metadata);

        // validate and compare internal vs. external pruning
        verifyEquality(internal, external);
    }

    private void testInternalPrune(String query, String expected) {
        testInternalPrune(query, expected, typeMetadata);
    }

    private ASTJexlScript testInternalPrune(String query, String expected, TypeMetadata metadata) {
        try {
            ASTJexlScript script = parseQuery(query);
            validator.isValid(script, "Internal Prune: query");
            ASTJexlScript pruned = (ASTJexlScript) IngestTypePruningVisitor.prune(script, metadata);

            log.info("input   : " + query);
            log.info("output  : " + JexlStringBuildingVisitor.buildQuery(pruned));
            log.info("expected: " + expected);

            // all pruned scripts must be valid
            assertTrue(validator.isValid(pruned, "Internal Prune: result"));

            // we might be expecting nothing as a result
            if (expected == null) {
                log.trace("expected null! " + JexlStringBuildingVisitor.buildQuery(pruned));
                assertEquals("failed for query: " + query, 0, pruned.jjtGetNumChildren());
                return null;
            }

            ASTJexlScript expectedScript = parseQuery(expected);
            assertTrue(validator.isValid(expectedScript, "Internal Prune: expected"));
            verifyEquality(pruned, expectedScript);
            return pruned;
        } catch (Exception e) {
            e.printStackTrace();
            fail("test failed: " + e.getMessage());
        }
        return null;
    }

    private void testExternalPrune(String query, String expected) {
        testExternalPrune(query, expected, typeMetadata);
    }

    private void testExternalPrune(String query, String expected, Set<String> ingestTypes) {
        testExternalPrune(query, expected, typeMetadata, ingestTypes);
    }

    private ASTJexlScript testExternalPrune(String query, String expected, TypeMetadata metadata) {
        ASTJexlScript script = parseQuery(query);
        Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(script, metadata);
        return testExternalPrune(query, expected, metadata, ingestTypes);
    }

    private ASTJexlScript testExternalPrune(String query, String expected, TypeMetadata metadata, Set<String> ingestTypes) {
        try {
            ASTJexlScript script = parseQuery(query);
            assertTrue(validator.isValid(script, "External Prune: query"));

            if (ingestTypes == null) {
                ingestTypes = IngestTypeVisitor.getIngestTypes(script, metadata);
            }

            if (ingestTypes.contains(IGNORED_TYPE)) {
                return parseQuery(expected);
            }

            ASTJexlScript pruned = (ASTJexlScript) IngestTypePruningVisitor.prune(script, metadata, ingestTypes);

            log.info("input   : " + query);
            log.info("output  : " + JexlStringBuildingVisitor.buildQuery(pruned));
            log.info("expected: " + expected);

            // all pruned scripts must be valid
            assertTrue(validator.isValid(pruned, "External Prune: result"));

            // we might be expecting nothing as a result
            if (expected == null) {
                log.trace("expected null! " + JexlStringBuildingVisitor.buildQuery(pruned));
                assertEquals("failed for query: " + query, 0, pruned.jjtGetNumChildren());
                return null;
            }

            ASTJexlScript expectedScript = parseQuery(expected);
            assertTrue(validator.isValid(expectedScript, "External Prune: expected"));
            verifyEquality(pruned, expectedScript);
            return pruned;
        } catch (Exception e) {
            e.printStackTrace();
            fail("test failed: " + e.getMessage());
        }
        return null;
    }

    private ASTJexlScript parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    private void verifyEquality(ASTJexlScript script, ASTJexlScript expected) {
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expected, script);
        assertTrue("Jexl tree comparison failed with reason: " + comparison.getReason(), comparison.isEqual());
    }

    // --- THE SETH ZONE ---

    // Helper Methods
    public static AccumuloClient setupAccumulo() throws Exception {

        final String SHARD_INDEX = "shardIndex";

        AccumuloClient client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD_INDEX);

        BatchWriter bw = client.createBatchWriter(SHARD_INDEX,
                new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(1));

        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        Uid.List list = builder.build();

        Mutation m = new Mutation("ba");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("234");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("bag");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("234");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("candy corn");
        m.put(new Text("CANDY_TYPE"), new Text("20190315\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("bar");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.addUID("456");
        builder.addUID("567");
        builder.setIGNORE(false);
        builder.setCOUNT(3);
        list = builder.build();

        m = new Mutation("bard");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype2"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("bardy");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.addUID("456");
        builder.addUID("567");
        builder.addUID("1345");
        builder.addUID("2456");
        builder.addUID("3567");
        builder.setIGNORE(false);
        builder.setCOUNT(6);
        list = builder.build();

        m = new Mutation("boohoo");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        // Too many laughs
        builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(30);
        list = builder.build();

        m = new Mutation("bahahaha");
        m.put(new Text("LAUGH"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("567");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("barz");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("678");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("bat");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("efg");
        builder.addUID("fgh");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE1");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("def");
        builder.addUID("egh");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE2");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("cde");
        builder.addUID("def");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE3");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("bcd");
        builder.addUID("cde");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE4");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("abc");
        builder.addUID("bcd");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE5");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("negnum1");
        builder.addUID("negnum2");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation(NumericalEncoder.encode("-1"));
        m.put(new Text("KELVIN"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("barter");
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("baggy");
        m.put(new Text("FOO"), new Text("20190414_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("oreo");
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        // Terms for high-low cardinality test with query (FOO == 'low_card' && FOO == 'high_card')
        // Four terms {'highest_card', 'high_card', 'low_card', 'lowest_card'}
        // Ranges fall across 8 days, each day has up to 50 shards.
        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("lowest_card");
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_22\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_49\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("low_card");
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190312_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_22\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_33\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190317_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.addUID("d.e.f");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("high_card");
        for (int day = 0; day < 8; day += 2) {
            for (int ii = 1; ii < 50; ii++) {
                m.put(new Text("FOO"), new Text("2019031" + day + "_" + ii + "\0" + "datatype1"), new Value(list.toByteArray()));
            }
        }
        bw.addMutation(m);

        m = new Mutation("highest_card");
        for (int day = 0; day < 8; day++) {
            for (int ii = 1; ii < 50; ii++) {
                m.put(new Text("FOO"), new Text("2019031" + day + "_" + ii + "\0" + "datatype1"), new Value(list.toByteArray()));
            }
        }
        bw.addMutation(m);

        // ---------------

        // Keep it simple, just have one hit.
        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(true);
        builder.setCOUNT(5000);
        list = builder.build();

        // With shards per day set to zero, these will roll up
        m = new Mutation("day_ranges");
        m.put(new Text("FOO"), new Text("20190310_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_2\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_3\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_4\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_5\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_6\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_7\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_8\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190311_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190312_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190313_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190315_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_2\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_3\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_4\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_5\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_6\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_7\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_8\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190316_0\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("shard_range");
        m.put(new Text("FOO"), new Text("20190310_21\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_51\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        bw.flush();
        bw.close();

        return client;
    }
    private RangeStream getRangeStream(MetadataHelper helper, ShardQueryConfiguration config) {
        ScannerFactory scannerFactory = new ScannerFactory(config);
        return new RangeStream(config, scannerFactory, helper);
    }

    // Seth's first try
    @Test
    public void testAfterRangeStream() throws Exception {

        /**
         *
         The query tree pre-RangeStream has invalid node parentage
         The query tree post-RangeStream has invalid node parentage
         The IngestTypePruningVisitor breaks node parentage during it's operation
         Some unhandled edge case in the IngestTypePruningVisitor

         */
        //( (NAME:value) OR (NAME:value) OR (AGE:value AND AGE:value) OR (HEIGHT:regex or HEIGHT:regex) ) AND (#INTERSECTS() OR #INTERSECTS())
        // Set up range stream

        AccumuloClient client = setupAccumulo();
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setClient(client);

        String originalQuery = "(FOO == 'oreo') && ((filter:include(FOO, 'tardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) || (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) )";

        //String originalQuery = "(FOO == 'oreo')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = Sets.newHashSet(makeShardedRange("20190314_1"));

        for (QueryPlan queryPlan : getRangeStream(helper, config).streamPlans(script)) {
            // verify the query plan dropped no terms
            JexlNode queryTree = JexlASTHelper.parseJexlQuery(queryPlan.getQueryString());
            PrintingVisitor.printQuery(queryTree);
            JexlNode expectedTree = JexlASTHelper.parseJexlQuery(
                    "(((SHARDS_AND_DAYS = '20190314') && filter:include(FOO, 'tardy')) || ((SHARDS_AND_DAYS = '20190314') && filter:include(FOO, 'bardy'))) && FOO == 'oreo'");
            JexlNodeAssert.assertThat(queryTree).isEqualTo(expectedTree);

            // verify the range
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges, expectedRanges.remove(range));
            }
        }

        assertTrue(expectedRanges.size() + " expected ranges not found inquery plan: " + expectedRanges, expectedRanges.isEmpty());


        //Now that the range stream is set up, we need to get the Query in jexl format and send that into the test() method.
        // this is also a good spot to se what exactly is being produced from the range stream, because that can help us single out if the problem is there in here in ingesttyupepruiningcidistlsesasdf

        String jexlQueryAfterRangeStream = JexlStringBuildingVisitor.buildQuery(JexlASTHelper.parseJexlQuery(getRangeStream(helper, config).streamPlans(script).iterator().next().getQueryString()));
        System.out.println("Post Range Stream Query: " + jexlQueryAfterRangeStream);
        //assertEquals(jexlQueryAfterRangeStream, originalQuery);

        boolean validLineage = JexlASTHelper.validateLineage(JexlASTHelper.parseJexlQuery(getRangeStream(helper, config).streamPlans(script).iterator().next().getQueryString()), false);
        System.out.println("IS valid lineage? " + validLineage);
    }

    // Laura's test
    @Test
    public void testPruneFirstTermOfJunction() {
        // prune C term
        String query = "A == '1' && (B == '2' || C == '3')";
        String expected = "A == '1' && B == '2'";
        test(query, expected);

        // prune C term
        query = "((B == '2' || C == '3') && A == '1')";
        expected = "B == '2' && A == '1'";
        test(query, expected);

        test("( !(C =='3') && A == '1')", "A == '1'");
        test("!(C =='3') && A == '1'", "A == '1'");

    }

    // Post-talk tests
    @Test
    public void testForQueryModelExpansionFailure() throws ParseException, InvalidQueryTreeException {

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        // TODO: Figure out what ModelFieldAttributes means. "AG" was used as a term in the model.
        // model.setModelFieldAttribute("AG", QueryModel.LENIENT);

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");

        String original = "((A == 'Ants' || A == 'Apes' || (B == 'Bees' && B('Birds')) || (C == 'Cats' || C == 'Camel')) && (INTERSECTS() || INTERSECTS()))";

        ASTJexlScript parsedAndFlattenedJexl = JexlASTHelper.parseAndFlattenJexlQuery(original);

        System.out.println(" --- Original Jexl Query --- ");
        PrintingVisitor.printQuery(original);

        System.out.println(" --- Parsed and Flattened Jexl Query --- ");
        PrintingVisitor.printQuery(parsedAndFlattenedJexl);

        validator.isValid(parsedAndFlattenedJexl);

    }

    // Queries that throw an error here are good ones to use in other tests since they'll
    // make some noise at the place you're looking for.
    // There doesn't seem to be much of a difference in this specific error if I include the QueryModelVisitor or not.
    @Test
    public void testQueriesWithoutTreeFlattening() throws ParseException, InvalidQueryTreeException {

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        // TODO: Figure out what ModelFieldAttributes means. "AG" was used as a term in the model.
        // model.setModelFieldAttribute("AG", QueryModel.LENIENT);

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");

        // String original = "A == 'Ants' || A == 'Apes' || (B == 'Bees' && B('Birds')) || (C == 'Cats' || C == 'Camel')";
        // String original = "A == '1' || A == '2' || (B == '2' && B('3')) || (C == '3' || C == '4')";


        // These failures are (I believe) from not using TreeFlatteningRebuildingVisitor
        // String original = "   A == '1'";                                         // Works fine
        // String original = "  (A == '1')";                                        // Works fine
        // String original = "   A == '1' || A == '2' || B == '2'";                 // Works fine
        // String original = "  (A == '1' || B == '1')";                            // Works fine
        // String original = "   A == '1' || A == '2' || (B == '2')";               // Internal Prune: query produced an invalid query tree: [RefExpr]
        // String original = "   A == '1' || A == '2' || C == '3' || C == '4'";     // Works fine
        // String original = "   A == '1' || A == '2' || (C == '3') || C == '4'";   // Internal Prune: query produced an invalid query tree: [RefExpr]
        // String original = "  (A == '1' || (B == '1'))";                          // Internal Prune: query produced an invalid query tree: [RefExpr]
        //String original = "   (B == '1')";                                        // Works fine
        //String original = "   (A == '1' || !(B == '1'))";                         // Works fine
        String original = "(A == '1' && (B == '1'))";                               // Internal Prune: query produced an invalid query tree: [RefExpr]

        // For some reason, "parseAndFlatten" isn't removing the parentheses around the B == '1'.
        // Maybe I'm missing something, but for now it seems like the flattening isn't working.
        // FIX: see TreeFlatteningRebuildingVisitor. pAFJQ doesn't rebuild.
        ASTJexlScript parsedAndFlattenedJexl = JexlASTHelper.parseAndFlattenJexlQuery(original);

        System.out.println(" --- Original Jexl Query --- ");
        PrintingVisitor.printQuery(original);

        System.out.println(" --- Parsed and Flattened Jexl Query --- ");
        PrintingVisitor.printQuery(parsedAndFlattenedJexl);

        test(JexlStringBuildingVisitor.buildQuery(parsedAndFlattenedJexl), "SUCCESS");
    }


    @Test
    public void testExpansionPruning() throws ParseException, InvalidQueryTreeException {

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        // TODO: Figure out what ModelFieldAttributes means. "AG" was used as a term in the model.
        // model.setModelFieldAttribute("AG", QueryModel.LENIENT);

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");

        // String original = "A == 'Ants' || A == 'Apes' || (B == 'Bees' && B('Birds')) || (C == 'Cats' || C == 'Camel')";
        // String original = "A == '1' || A == '2' || (B == '2' && B('3')) || (C == '3' || C == '4')";


        // These failures are (I believe) from not using TreeFlatteningRebuildingVisitor
        //String original = "   A == '1'";                                         // Works fine
        //String original = "  (A == '1')";                                        // Works fine
        //String original = "   A == '1' || A == '2' || B == '2'";                 // Works fine
        //String original = "  (A == '1' || B == '1')";                            // Works fine
        //String original = "   A == '1' || A == '2' || (B == '2')";               // Works find
        //String original = "   A == '1' || A == '2' || C == '3' || C == '4'";     // Works fine
        //String original = "   A == '1' || A == '2' || (C == '3') || C == '4'";   //Works fine
        //String original = "  (A == '1' || (B == '1'))";                          // Works fine
        //String original = "  (B == '1')";                                        // Works fine
        //String original = "  (A == '1' || !(B == '1'))";                         // Works fine
        String original = "  (A == '1' && (B == '1'))";                          // Works fine

        // For some reason, "parseAndFlatten" isn't removing the parentheses around the B == '1'.
        // Maybe I'm missing something, but for now it seems like the flattening isn't working.
        // FIX: see TreeFlatteningRebuildingVisitor. pAFJQ doesn't rebuild.
        ASTJexlScript parsedAndFlattenedJexl = JexlASTHelper.parseAndFlattenJexlQuery(original);
        ASTJexlScript treeFlattenedJexl = TreeFlatteningRebuildingVisitor.flattenAll(parsedAndFlattenedJexl);

        System.out.println(" --- Original Jexl Query --- ");
        PrintingVisitor.printQuery(original);

        System.out.println(" --- Parsed and Flattened Jexl Query --- ");
        PrintingVisitor.printQuery(parsedAndFlattenedJexl);

        System.out.println(" --- Tree-Flattened Jexl Query --- ");
        PrintingVisitor.printQuery(treeFlattenedJexl);

        ASTJexlScript queryModelAppliedJexl = QueryModelVisitor.applyModel(treeFlattenedJexl, model, allFields);
        System.out.println(" --- QueryModel-Applied Query --- ");
        PrintingVisitor.printQuery(queryModelAppliedJexl);

        test(JexlStringBuildingVisitor.buildQuery(queryModelAppliedJexl), "SUCCESS");
    }

    @Test
    public void findQueryThatMakesQueryModelVisitorProduceAnUnflattenedTree() throws InvalidQueryTreeException, ParseException {
        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        // TODO: Figure out what ModelFieldAttributes means. "AG" was used as a term in the model.
        // model.setModelFieldAttribute("AG", QueryModel.LENIENT);

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");

        // String original = "A == 'Ants' || A == 'Apes' || (B == 'Bees' && B('Birds')) || (C == 'Cats' || C == 'Camel')";
        String original = "A == '1' || A == '2' || (B == '2' && B('3')) || (C == '3' || C == '4')";


        // These failures are (I believe) from not using TreeFlatteningRebuildingVisitor
        //String original = "   A == '1'";                                         // Works fine
        //String original = "  (A == '1')";                                        // Works fine
        //String original = "   A == '1' || A == '2' || B == '2'";                 // Works fine
        //String original = "  (A == '1' || B == '1')";                            // Works fine
        //String original = "   A == '1' || A == '2' || (B == '2')";               // Works find
        //String original = "   A == '1' || A == '2' || C == '3' || C == '4'";     // Works fine
        //String original = "   A == '1' || A == '2' || (C == '3') || C == '4'";   //Works fine
        //String original = "  (A == '1' || (B == '1'))";                          // Works fine
        //String original = "  (B == '1')";                                        // Works fine
        //String original = "  (A == '1' || !(B == '1'))";                         // Works fine
        //String original = "  (A == '1' && (B == '1'))";                          // Works fine

        // For some reason, "parseAndFlatten" isn't removing the parentheses around the B == '1'.
        // Maybe I'm missing something, but for now it seems like the flattening isn't working.
        // FIX: see TreeFlatteningRebuildingVisitor. pAFJQ doesn't rebuild.
        ASTJexlScript parsedAndFlattenedJexl = JexlASTHelper.parseAndFlattenJexlQuery(original);
        //ASTJexlScript treeFlattenedJexl = TreeFlatteningRebuildingVisitor.flattenAll(parsedAndFlattenedJexl);

//        System.out.println(" --- Original Jexl Query --- ");
//        PrintingVisitor.printQuery(original);
//
        System.out.println(" --- Parsed and Flattened Jexl Query --- ");
        PrintingVisitor.printQuery(parsedAndFlattenedJexl);

//        assertFalse("Nope, works fine.", validator.isValid(parsedAndFlattenedJexl));

        ASTJexlScript queryModelAppliedJexl = QueryModelVisitor.applyModel(parsedAndFlattenedJexl, model, allFields);
        System.out.println(" --- QueryModel-Applied Query --- ");
        PrintingVisitor.printQuery(queryModelAppliedJexl);

        // check if the QMV messes up the validity of the query.
        validator.setValidateFlatten(true);
        validator.setValidateJunctions(false);
        validator.setValidateLineage(false);
        validator.setValidateReferenceExpressions(false);
        validator.setValidateQueryPropertyMarkers(false);
        assertFalse("Nope, works fine.", validator.isValid(queryModelAppliedJexl));
    }

    @Test
    public void testWhoopsUsedLuceneForTheFunction() throws ParseException {

        TypeMetadata tm = new TypeMetadata();
        tm.put("A", "ingestType1", LcType.class.getTypeName());
        tm.put("A", "ingestType2", LcType.class.getTypeName());
        tm.put("A", "ingestType3", LcType.class.getTypeName());
        tm.put("B", "ingestType1", LcType.class.getTypeName());
        tm.put("B", "ingestType2", LcType.class.getTypeName());
        tm.put("C", "ingestType5", LcType.class.getTypeName());
        tm.put("123", "ingestType1", LcType.class.getTypeName());

//        test("( !(C =='3') && A == '1')", "A == '1'", tm);

        // Found something!! Looks like the B('3') part is causing the null error. I need to check what the syntax should be for this.
        // ... uh... why was that there to begin with :/ ???
        // Oh it was a function. swapped with INTERSECTS
        // #doesnt work
        String original = "A == '1' || A == '2' || (B == '2' && B == '3') || INTERSECTS(FIELD, 'POINT(10 20)')";// || (C == '3' || C == '4')"; // as-is causes test to null-pointer
        //String original = "(A == '1') || A == '2'"; // this is a different error caused by not tree-ifying visitoring
        String flat = JexlStringBuildingVisitor.buildQueryWithoutParse(JexlASTHelper.parseAndFlattenJexlQuery(original)); // same null here
        String treed = JexlStringBuildingVisitor.buildQueryWithoutParse(TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseAndFlattenJexlQuery(original))); // same null here

        PrintingVisitor.printQuery(treed);
        test(treed, "A == '1'", tm);
    }

    @Test
    public void testNowUsingActualJexlAndDoTheQMStuff() throws ParseException, InvalidQueryTreeException {


        // --- VALIDATION ---

        validator.setValidateFlatten(true);
        validator.setValidateJunctions(false);
        validator.setValidateLineage(false);
        validator.setValidateReferenceExpressions(false);
        validator.setValidateQueryPropertyMarkers(false);

        // --- TYPE METADATA (removing stuff) ---

        TypeMetadata tm = new TypeMetadata();
        tm.put("A", "ingestType1", LcType.class.getTypeName());
        tm.put("A", "ingestType2", LcType.class.getTypeName());
        tm.put("A", "ingestType3", LcType.class.getTypeName());
        tm.put("B", "ingestType1", LcType.class.getTypeName());
        tm.put("B", "ingestType2", LcType.class.getTypeName());
        tm.put("C", "ingestType5", LcType.class.getTypeName());
        tm.put("123", "ingestType1", LcType.class.getTypeName());
        tm.put("", "ingestType1", LcType.class.getTypeName());
        tm.put("", "ingestType2", LcType.class.getTypeName());
        tm.put("", "ingestType3", LcType.class.getTypeName());
        tm.put("B", "ingestType1", LcType.class.getTypeName());
        tm.put("B", "ingestType2", LcType.class.getTypeName());
        tm.put("C1", "ingestType5", LcType.class.getTypeName());
        tm.put("C2", "ingestType5", LcType.class.getTypeName());
        tm.put("123", "ingestType1", LcType.class.getTypeName());

        // --- Remapping / changing stuff ---

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");
        // whoops, i need to add the mappings here too
        allFields.add("C1");
        allFields.add("C2");

        String original = "A == '1' || A == '2' || (B == '2' && geowave:intersects(FIELD, 'POINT(10 20)')) || (C == '3' && C == '4')";
        test(original, "SUCCESS", tm);

        // replace..?
        // ok ill groom it sheesh
        // hmmm i dont think i need the grooming. the allFields thing fixed the issue.
        ASTJexlScript queryModelAppliedJexl = QueryModelVisitor.applyModel(JexlASTHelper.parseJexlQuery(original), model, allFields);

        System.out.println(" --- QueryModel-Applied Query --- ");
        System.out.println("Is queryModelAppliedJexl valid? | " + validator.isValid(queryModelAppliedJexl));
        PrintingVisitor.printQuery(queryModelAppliedJexl);

        test(JexlStringBuildingVisitor.buildQuery(queryModelAppliedJexl), "SUCCESS", tm);

    }


    @Test
    public void testQueryModelRemapWorks() throws ParseException, InvalidQueryTreeException {

        // --- VALIDATION ---

        validator.setValidateFlatten(true);
        validator.setValidateJunctions(false);
        validator.setValidateLineage(false);
        validator.setValidateReferenceExpressions(false);
        validator.setValidateQueryPropertyMarkers(false);

        // --- Remapping / changing stuff ---

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");
        // whoops, i need to add the mappings here too
        allFields.add("C1");
        allFields.add("C2");

        String original = "A == '1' || A == '2' || (B == '2' && geowave:intersects(FIELD, 'POINT(10 20)')) || (C == '3' && C == '4')";

        // MUST use invert to groom it. Don't forget!
        ASTJexlScript groomed = InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
        ASTJexlScript queryModelAppliedJexl = QueryModelVisitor.applyModel(groomed, model, allFields);

        System.out.println(" --- QueryModel-Applied Query --- ");
        System.out.println("Is queryModelAppliedJexl valid? | " + validator.isValid(queryModelAppliedJexl));
        PrintingVisitor.printQuery(queryModelAppliedJexl);

    }

    @Test
    public void testMyUnderstanding() throws ParseException, InvalidQueryTreeException {


        // --- VALIDATION ---

        validator.setValidateFlatten(true);
        validator.setValidateJunctions(false);
        validator.setValidateLineage(false);
        validator.setValidateReferenceExpressions(false);
        validator.setValidateQueryPropertyMarkers(false);

        // --- TYPE METADATA (removing stuff) ---

        TypeMetadata tm = new TypeMetadata();
        tm.put("A", "ingestType1", LcType.class.getTypeName());
        tm.put("B", "ingestType3", LcType.class.getTypeName());
        tm.put("C", "ingestType2", LcType.class.getTypeName());


        // --- Remapping / changing stuff ---
        // exclusive negated term evaluates to true, drop the whole union

        // These have different behaviour :O
//        "A == '1' && (B == '2' || !(C == '3'))";  ->  A == '1'
//        "A == '1' && (B == '2' || C != '3')";     ->  A == '1' && B == '2'


        String query = "C == 'PICKLE' || (B == 'PICKLE' && A == 'PICKLE')";
        String expected = "A == '1'";
        test(query, expected, tm);

//        String original = "A == '1' && (A == '2' || A != '5')";
//        test(original, "SUCCESS", tm);
        //        String original = "(A == 'PICKLE!') OR (B == 'PICKLE!') OR (C == 'PICKLE!' AND C == 'PICKLE!') OR (D == '([A-Za-z0-9]+( [A-Za-z0-9]+)+)!' OR D == '([0-9]+( [0-9]+)+)!)' ) AND (geowave:intersects(FIELD, 'POINT(10 20)') OR geowave:intersects(MEADOW, 'POINT(1000 20)'))";


    }

    @Test
    public void testTM() throws ParseException, InvalidQueryTreeException {

        // --- VALIDATION ---

        validator.setValidateFlatten(true);
        validator.setValidateJunctions(false);
        validator.setValidateLineage(false);
        validator.setValidateReferenceExpressions(false);
        validator.setValidateQueryPropertyMarkers(false);



        // --- GENERATOR ---

        Set<String> valueSet = new HashSet<>();
        valueSet.add("pickle");
        valueSet.add("banana");
        valueSet.add("carrot");

        HashSet<String> fieldSet = new HashSet<>();
        fieldSet.add("A");
        fieldSet.add("B");
        fieldSet.add("C");
        fieldSet.add("D");

        // !!!
        JexlQueryGenerator generator = new JexlQueryGenerator(fieldSet, valueSet);
        generator.enableAllOptions();
        String original = generator.getQuery(100);



        // --- REMAPPING (QM) ---

        // Create a query model that has exactly the mappings we need to test edge cases.
        QueryModel model = new QueryModel();

        // Forward Mappings
        model.addTermToModel("A", "");
        model.addTermToModel("B", "B");
        model.addTermToModel("C", "C1");
        model.addTermToModel("C", "C2");
        model.addTermToModel("C", "C3");
        model.addTermToModel("D", "Dork");
        model.addTermToModel("C2", "Dork");

        // Reverse Mappings (Mirror of Forward Mappings)
        model.addTermToReverseModel("", "A");
        model.addTermToReverseModel("B", "B");
        model.addTermToReverseModel("C1", "C");
        model.addTermToReverseModel("C2", "C");
        model.addTermToReverseModel("C3", "C");
        model.addTermToReverseModel("Dork", "D");
        model.addTermToReverseModel("C2", "Dork");

        HashSet<String> allFields = new HashSet<>();
        allFields.add("A");
        allFields.add("B");
        allFields.add("C");
        allFields.add("C1");
        allFields.add("C2");
        allFields.add("C3");
        allFields.add("D");
        allFields.add("Dork");

        // !!!
        ASTJexlScript groomed = InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
        ASTJexlScript queryModelAppliedJexl = QueryModelVisitor.applyModel(groomed, model, allFields);

        System.out.println(" --- QUERY MODEL STUFF --- ");
        System.out.println("POST-QM VALID? (You're trying to make this FALSE) -> | " + validator.isValid(queryModelAppliedJexl));
        PrintingVisitor.printQuery(queryModelAppliedJexl);

        // --- Removing Stuff (TM) ---

        TypeMetadata tm = new TypeMetadata();
        tm.put("A",     "ingestType1", LcType.class.getTypeName());
        tm.put("B",     "ingestType1", LcType.class.getTypeName());
        tm.put("C",     "ingestType1", LcType.class.getTypeName());
        tm.put("D",     "ingestType2", LcType.class.getTypeName());
        tm.put("C1",    "ingestType1", LcType.class.getTypeName());
        tm.put("C2",    "ingestType1", LcType.class.getTypeName());
        tm.put("C3",    "ingestType1", LcType.class.getTypeName());
        tm.put("Dork",  "ingestType2", LcType.class.getTypeName());

        // !!!
        System.out.println(" --- TYPE METADATA STUFF --- ");
        test(JexlStringBuildingVisitor.buildQuery(queryModelAppliedJexl), "", tm);

    }
}

// In case you forget again :)
// JexlStringBuildingVisitor.buildQuery(<root-node>)
// QUERYMODEL RE-MAPS (C -> C1 C2)

// QueryModel applied first, then its output is piped to the TM removal process
// Replace -> Trim
// QM Replace -> Unflattened -> TM Trim -> Error!

// 1. QMV.ApplyModel()
// 2. ApplyModel flattens the script
// 3. THEN it does jjtAccept!!!
