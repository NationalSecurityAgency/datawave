package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Category;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

public class QueryAnalyzerStackOverflowTest {
    @Test
    public void analyzesWideJunctionsWithoutLosingTerms() {
        int size = 15000;
        List<String> terms = new ArrayList<>(Collections.nCopies(size, "A == 'x'"));
        terms.set(0, "FIRST == 'x'");
        terms.set(size / 2, "MIDDLE == 'x'");
        terms.set(size - 1, "LAST == 'x'");
        QueryAnalyzer analyzer = new QueryAnalyzer();
        for (String operator : List.of(" && ", " || ")) {
            String flat = String.join(operator, terms);
            List<String> groups = new ArrayList<>();
            // Small groups produce an equivalent tree without a long chain of junction nodes.
            for (int i = 0; i < size; i += 100) {
                groups.add("(" + String.join(operator, terms.subList(i, i + 100)) + ")");
            }
            Collections.reverse(groups);
            List<QueryAnalysis> results = analyzer.analyze(List.of(flat, String.join(operator, groups)), Syntax.JEXL).getQueries();
            for (QueryAnalysis result : results) {
                assertEquals(result.getError(), Status.SUCCESS, result.getStatus());
                assertNotNull(result.getSignature());
                assertNotNull(result.getFingerprint());
                assertEquals(Set.of("A", "FIRST", "MIDDLE", "LAST"), result.getFields());
                assertEquals(EnumSet.of(Category.EQUALITY, operator.contains("&&") ? Category.CONJUNCTION : Category.DISJUNCTION), result.getCategories());
                assertEquals(size, result.getFingerprint().getMeasurements().get("predicates"), 0);
                assertEquals(size, result.getFingerprint().getMeasurements().get("junctionWidth"), 0);
                assertEquals(1, result.getFingerprint().getMeasurements().get("booleanDepth"), 0);
                assertEquals(size - 4, result.getFingerprint().getMeasurements().get("repeatedFieldUses"), 0);
            }
            assertEquivalent(results.get(0), results.get(1));
        }
    }

    @Test
    public void preservesMarkersAndDuplicateTermsWhenFlattening() {
        String marker = "((_Delayed_ = true) && (A == 'x' && B == 2))";
        QueryAnalyzer analyzer = new QueryAnalyzer();
        for (String operator : List.of(" && ", " || ")) {
            List<QueryAnalysis> results = analyzer
                            .analyze(List.of(marker + operator + "C == 3" + operator + "C == 4", "C == 5" + operator + "(" + marker + operator + "C == 6)",
                                            "(A == 'x' && B == 2)" + operator + "C == 3" + operator + "C == 4", marker + operator + "C == 3"), Syntax.JEXL)
                            .getQueries();
            for (QueryAnalysis result : results) {
                assertEquals(result.getError(), Status.SUCCESS, result.getStatus());
            }
            assertEquivalent(results.get(0), results.get(1));
            assertTrue(results.get(0).getCategories().contains(Category.PROPERTY_MARKER));
            assertEquals(Set.of("A", "B", "C"), results.get(0).getFields());
            for (int i = 2; i < results.size(); i++) {
                assertNotEquals(results.get(0).getSignature(), results.get(i).getSignature());
                assertNotEquals(results.get(0).getFingerprint().getKey(), results.get(i).getFingerprint().getKey());
            }
        }
    }

    @Test
    public void isolatesLuceneConversionOverflowAndReusesAnalyzer() {
        QueryAnalyzer analyzer = new QueryAnalyzer(new LuceneToJexlQueryParser() {
            @Override
            public QueryNode parse(String query) throws ParseException {
                if ("OVERFLOW".equals(query)) {
                    throw new StackOverflowError();
                }
                return super.parse(query);
            }
        });
        assertOverflowIsolated(analyzer, new QueryInput("OVERFLOW", Syntax.LUCENE));
    }

    @Test
    public void propagatesOtherErrors() {
        AssertionError error = new AssertionError("Unexpected parser failure");
        QueryAnalyzer analyzer = new QueryAnalyzer(new LuceneToJexlQueryParser() {
            @Override
            public QueryNode parse(String query) {
                throw error;
            }
        });
        assertSame(error, assertThrows(AssertionError.class, () -> analyzer.analyze(List.of("NAME:value"), Syntax.LUCENE)));
    }

    @Test
    public void isolatesDeepJexlQueriesWithBoundedStack() throws Exception {
        Path output = Files.createTempFile("query-analyzer-stack-overflow-", ".log");
        Process process = null;
        try {
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xss512k", "-Xmx256m", "-cp", classpath,
                            DeepQueryProbe.class.getName()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            assertTrue("Deep-query probe timed out", process.waitFor(30, TimeUnit.SECONDS));
            assertEquals(Files.readString(output), 0, process.exitValue());
        } finally {
            if (process != null) {
                process.destroyForcibly();
            }
            Files.deleteIfExists(output);
        }
    }

    /** Run real parser overflows with a fixed stack instead of depending on the test runner's stack size or compilation state. */
    public static class DeepQueryProbe {
        public static void main(String[] args) {
            QueryAnalyzer analyzer = new QueryAnalyzer();
            for (String opening : List.of("(", "!(")) {
                String query = opening.repeat(10000) + "NAME == 'deep'" + ")".repeat(10000);
                assertThrows(StackOverflowError.class, () -> JexlASTHelper.parseJexlQuery(query));
                assertOverflowIsolated(analyzer, new QueryInput(query, Syntax.JEXL));
            }
        }
    }

    private static void assertOverflowIsolated(QueryAnalyzer analyzer, QueryInput overflowing) {
        List<QueryInput> inputs = List.of(new QueryInput("NAME:before", Syntax.LUCENE), overflowing, new QueryInput("NAME:after", Syntax.LUCENE));
        AnalysisReport report = analyzer.analyze(inputs);
        List<QueryAnalysis> results = report.getQueries();
        assertEquals(inputs.size(), results.size());
        for (int i = 0; i < inputs.size(); i++) {
            assertEquals(i, results.get(i).getIndex());
            assertSame(inputs.get(i), results.get(i).getInput());
        }
        QueryAnalysis failed = results.get(1);
        assertEquals(Status.UNSUPPORTED, failed.getStatus());
        assertEquals("Query exceeds parser or analyzer stack depth limits", failed.getError());
        assertNull(failed.getJexl());
        assertNull(failed.getSignature());
        assertNull(failed.getFingerprint());
        assertTrue(failed.getCategories().isEmpty());
        assertTrue(failed.getFields().isEmpty());
        assertTrue(failed.getFunctions().isEmpty());
        assertEquals(Status.SUCCESS, results.get(0).getStatus());
        assertEquals(Status.SUCCESS, results.get(2).getStatus());
        assertEquals(1, report.getClusters().size());
        assertEquals(List.of(results.get(0), results.get(2)), report.getClusters().get(results.get(0).getSignature()));
        QueryClusterer.Result clustered = new QueryClusterer().cluster(report);
        assertEquals(2, clustered.getSuccessfulCount());
        assertEquals(1, clustered.getUnsupportedCount());
        assertEquals(0, clustered.getInvalidCount());
        assertEquals(Status.SUCCESS, analyzer.analyze(List.of("NAME:again"), Syntax.LUCENE).getQueries().get(0).getStatus());
    }

    private static void assertEquivalent(QueryAnalysis expected, QueryAnalysis actual) {
        assertEquals(expected.getSignature(), actual.getSignature());
        assertEquals(expected.getCategories(), actual.getCategories());
        assertEquals(expected.getFields(), actual.getFields());
        assertEquals(expected.getFunctions(), actual.getFunctions());
        QueryFingerprint left = expected.getFingerprint();
        QueryFingerprint right = actual.getFingerprint();
        assertEquals(left.getSignature(), right.getSignature());
        assertEquals(left.getKey(), right.getKey());
        assertEquals(left.getProtectedProfile(), right.getProtectedProfile());
        assertEquals(left.getBindings(), right.getBindings());
        assertEquals(left.getCounts(), right.getCounts());
        assertEquals(left.getTopology(), right.getTopology());
        assertEquals(left.getMeasurements(), right.getMeasurements());
        assertEquals(left.getProtections(), right.getProtections());
        assertEquals(left.getDiagnostics(), right.getDiagnostics());
    }
}
