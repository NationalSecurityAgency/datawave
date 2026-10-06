package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QueryAnalyzer.Syntax;

public class QueryFingerprintTest {
    @Test
    public void enrichesRegexWithoutChangingLegacySignatures() {
        List<QueryAnalysis> results = new QueryAnalyzer()
                        .analyze(Arrays.asList("NAME == 'alice'", "NAME =~ 'alice'", "NAME =~ 'al.*'", "NAME =~ '.*ce'", "NAME =~ 'a.*e'", "NAME =~ '.*lic.*'"),
                                        Syntax.JEXL)
                        .getQueries();
        String legacy = results.get(1).getSignature();
        for (int i = 1; i < results.size(); i++) {
            assertEquals(legacy, results.get(i).getSignature());
            for (int j = 0; j < i; j++) {
                assertFalse(QuerySimilarity.compare(results.get(i).getFingerprint(), results.get(j).getFingerprint()).isCompatible());
            }
        }
        assertTrue(results.get(2).getFingerprint().getProtections().contains("REGEX:PREFIX"));
        assertTrue(results.get(5).getFingerprint().getProtections().contains("REGEX:UNANCHORED"));
        assertEquals(2, results.get(2).getFingerprint().getVersion());
    }

    @Test
    public void preservesReorderingFlatteningAndLuceneEquivalence() {
        List<QueryAnalysis> results = new QueryAnalyzer().analyze(Arrays.asList(new QueryInput("NAME:al* AND (CITY:paris AND AGE:30)", Syntax.LUCENE),
                        new QueryInput("AGE == '30' && NAME =~ 'bo.*' && CITY == 'rome'", Syntax.JEXL))).getQueries();
        assertEquals(results.get(0).getFingerprint().getKey(), results.get(1).getFingerprint().getKey());
        assertEquals(results.get(0).getFingerprint().getBindings(), results.get(1).getFingerprint().getBindings());
        assertEquals(results.get(0).getFingerprint().getCounts(), results.get(1).getFingerprint().getCounts());
        assertEquals(results.get(0).getFingerprint().getMeasurements(), results.get(1).getFingerprint().getMeasurements());
        QueryFingerprint longer = fingerprint("NAME =~ 'alice.*'");
        QueryFingerprint shorter = fingerprint("NAME =~ 'al.*'");
        assertNotEquals(longer.getKey(), shorter.getKey());
        assertTrue(QuerySimilarity.compare(longer, shorter).isCompatible());
    }

    @Test
    public void relaxesMultiplicityButPreservesBooleanShapeAndBindings() {
        QueryFingerprint two = fingerprint("A == 1 && B == 2");
        QueryFingerprint three = fingerprint("A == 1 && B == 2 && C == 3");
        assertEquals(two.getProtectedProfile(), three.getProtectedProfile());
        assertNotEquals(two.getKey(), three.getKey());
        QuerySimilarity.Result similarity = QuerySimilarity.compare(two, three);
        assertTrue(similarity.getScore() >= 0.70);
        assertTrue(similarity.getScore() < 1);
        assertFalse(QuerySimilarity.compare(two, fingerprint("A == 1 || B == 2")).isCompatible());
        assertFalse(QuerySimilarity.compare(two, fingerprint("A == 1")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("(A == 1 && B == 2) || C == 3"), fingerprint("A == 1 && (B == 2 || C == 3)")).isCompatible());
        assertTrue(QuerySimilarity.compare(fingerprint("A == 1 && B == 2"), fingerprint("A == 1 && C == 3")).getScore() >= 0.70);
        assertTrue(QuerySimilarity.compare(fingerprint("A == 1"), fingerprint("B == 1")).getScore() < 0.70);
        assertTrue(QuerySimilarity.compare(fingerprint("A == 1 && B =~ 'x.*'"), fingerprint("B == 1 && A =~ 'x.*'")).getScore() < 0.70);
    }

    @Test
    public void regexCannotHideInsideManyEqualitiesOrFunctions() {
        StringBuilder base = new StringBuilder("A == 'x'");
        for (int i = 0; i < 100; i++) {
            base.append(" && FIELD_").append(i).append(" == 'x'");
        }
        QueryFingerprint equality = fingerprint(base.toString());
        QueryFingerprint regex = fingerprint(base + " && A =~ 'x.*'");
        QuerySimilarity.Result result = QuerySimilarity.compare(equality, regex);
        assertTrue(result.getScore() > 0.70);
        assertFalse(result.isCompatible());
        assertFalse(result.getProtectedDifferences().isEmpty());
        for (String function : List.of("includeRegex", "excludeRegex", "getAllMatches")) {
            assertFalse(QuerySimilarity.compare(fingerprint("filter:" + function + "(A, 'x.*')"), fingerprint("filter:" + function + "(A, '.*x.*')"))
                            .isCompatible());
        }
        assertFalse(QuerySimilarity
                        .compare(fingerprint("filter:matchesAtLeastCountOf(1, A, 'x.*')"), fingerprint("filter:matchesAtLeastCountOf(1, A, '.*x.*')"))
                        .isCompatible());
    }

    @Test
    public void retainsKnownFunctionControlsAndOrderedArguments() {
        QueryFingerprint any = fingerprint("filter:compare(A, '==', 'ANY', B)");
        assertEquals(any.getKey(), fingerprint("filter:compare(A, '=', 'any', B)").getKey());
        assertFalse(QuerySimilarity.compare(any, fingerprint("filter:compare(A, '==', 'ALL', B)")).isCompatible());
        assertFalse(QuerySimilarity.compare(any, fingerprint("filter:compare(A, '>', 'ANY', B)")).isCompatible());
        assertNotEquals(any.getKey(), fingerprint("filter:compare(B, '==', 'ANY', A)").getKey());
        QueryFingerprint within = fingerprint("content:within(TEXT, 3, termOffsetMap, 'quick', 'fox')");
        QueryFingerprint farther = fingerprint("content:within(TEXT, 10, termOffsetMap, 'quick', 'fox')");
        assertNotEquals(within.getKey(), farther.getKey());
        assertTrue(QuerySimilarity.compare(within, farther).isCompatible());
        assertTrue(QuerySimilarity.compare(within, farther).getScore() < 1);
        assertFalse(within.getBindings().stream().anyMatch(binding -> binding.contains("termOffsetMap")));
        assertFalse(QuerySimilarity.compare(within, fingerprint("content:within(3, termOffsetMap, 'quick', 'fox')")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("content:phrase(TEXT, termOffsetMap, 'quick', 'fox')"),
                        fingerprint("content:phrase(TEXT, termOffsetMap, 'quick', 'red', 'fox')")).isCompatible());
        assertNotEquals(fingerprint("filter:matchesAtLeastCountOf(1, A, 'x.*', 'y.*')").getKey(),
                        fingerprint("filter:matchesAtLeastCountOf(2, A, 'x.*', 'y.*')").getKey());
    }

    @Test
    public void retainsMarkersBoundsReservedFieldsAndOperandRoles() {
        QueryFingerprint bounded = fingerprint("((_Bounded_ = true) && (AGE >= 20 && AGE <= 40))");
        assertFalse(QuerySimilarity.compare(bounded, fingerprint("AGE >= 20 && AGE <= 40")).isCompatible());
        assertFalse(QuerySimilarity.compare(bounded, fingerprint("((_Bounded_ = true) && (AGE > 20 && AGE <= 40))")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("((_Delayed_ = true) && A == 1)"), fingerprint("((_Eval_ = true) && A == 1)")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("A == 1"), fingerprint("A == B")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("A == 1"), fingerprint("_ANYFIELD_ == 1")).isCompatible());
        assertFalse(QuerySimilarity.compare(fingerprint("A == 1"), fingerprint("_NOFIELD_ == 1")).isCompatible());
        assertNotEquals(fingerprint("A.1 == 1").getKey(), fingerprint("A.2 == 1").getKey());
    }

    @Test
    public void isolatesUnknownControlsWithoutBreakingLegacySuccess() {
        for (String query : List.of("A =~ PATTERN", "A =~ '['", "filter:compare(A, OPERATOR, 'ANY', B)", "content:within(DISTANCE, termOffsetMap, 'a', 'b')",
                        "filter:matchesAtLeastCountOf(1e999, A, 'a.*')", "((_Bounded_ = true) && (A >= 1 && B <= 5))")) {
            QueryFingerprint fingerprint = fingerprint(query);
            assertFalse(query, fingerprint.getDiagnostics().isEmpty());
            assertFalse(QuerySimilarity.compare(fingerprint, fingerprint(query + " && B == 1")).isCompatible());
        }
        QueryFingerprint a = fingerprint("filter:compare(A, 'invalid', 'ANY', B)");
        QueryFingerprint b = fingerprint("filter:compare(A, 'different', 'ANY', B)");
        assertFalse(QuerySimilarity.compare(a, b).isCompatible());
        List<QueryAnalysis> batch = new QueryAnalyzer().analyze(List.of("A == 1e999", "A == -1e999", "B == 'ok'"), Syntax.JEXL).getQueries();
        for (QueryAnalysis query : batch) {
            assertEquals(query.getError(), Status.SUCCESS, query.getStatus());
        }
    }

    @Test
    public void isolatesMixedNumericBoundsWithoutAbortingBatch() {
        List<String> queries = List.of("((_Bounded_ = true) && (AGE >= 1 && AGE <= 2.5))", "((_Bounded_ = true) && (AGE >= 1.5 && AGE <= 2))",
                        "((_Bounded_ = true) && (AGE >= 1 && AGE <= 3000000000))", "((_Bounded_ = true) && (AGE >= 1L && AGE <= 2))",
                        "((_Bounded_ = true) && (AGE >= 1.0 && AGE <= 2.5))", "AGE == 2");
        AnalysisReport report = new QueryAnalyzer().analyze(queries, Syntax.JEXL);
        assertEquals(queries.size(), report.getQueries().size());
        for (int i = 0; i < queries.size(); i++) {
            QueryAnalysis result = report.getQueries().get(i);
            assertEquals(i, result.getIndex());
            assertEquals(queries.get(i), result.getInput().getQuery());
            assertEquals(result.getError(), Status.SUCCESS, result.getStatus());
            if (i < 4) {
                assertFalse(queries.get(i), result.getFingerprint().getDiagnostics().isEmpty());
            } else {
                assertTrue(queries.get(i), result.getFingerprint().getDiagnostics().isEmpty());
            }
        }
        // Preserve legacy structural grouping, but keep uncertain ranges apart in planning-sensitive groups.
        assertEquals(2, report.getClusters().size());
        assertEquals(queries.size(), new QueryClusterer().cluster(report).getGroups().size());
    }

    @Test
    public void similarityIsSymmetricBoundedAndImmutable() {
        List<QueryFingerprint> fingerprints = Arrays.asList(fingerprint("A == 1"), fingerprint("A == 2 && B == 3"), fingerprint("B =~ '.*x'"),
                        fingerprint("filter:compare(A, '==', 'ANY', B)"));
        for (QueryFingerprint left : fingerprints) {
            assertEquals(1, QuerySimilarity.compare(left, left).getScore(), 1e-12);
            assertEquals(0, QuerySimilarity.distance(left, left), 1e-12);
            for (QueryFingerprint right : fingerprints) {
                QuerySimilarity.Result forward = QuerySimilarity.compare(left, right);
                assertEquals(forward.getScore(), QuerySimilarity.compare(right, left).getScore(), 1e-12);
                assertTrue(forward.getScore() >= 0 && forward.getScore() <= 1);
                assertEquals(QuerySimilarity.distance(left, right), QuerySimilarity.distance(right, left), 1e-12);
            }
        }
        assertThrows(UnsupportedOperationException.class, () -> fingerprints.get(0).getCounts().clear());
        assertThrows(UnsupportedOperationException.class, () -> fingerprints.get(0).getBindings().clear());
        assertThrows(UnsupportedOperationException.class, () -> fingerprints.get(0).getMeasurements().clear());
        assertThrows(UnsupportedOperationException.class, () -> QuerySimilarity.compare(fingerprints.get(0), fingerprints.get(1)).getComponents().clear());
    }

    static QueryFingerprint fingerprint(String query) {
        QueryAnalysis result = new QueryAnalyzer().analyze(List.of(query), Syntax.JEXL).getQueries().get(0);
        assertEquals(result.getError(), Status.SUCCESS, result.getStatus());
        return result.getFingerprint();
    }
}
