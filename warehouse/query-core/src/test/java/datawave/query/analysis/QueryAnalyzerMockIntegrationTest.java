package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Category;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryWorkloadSelector.Selection;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.Tuple3;

/**
 * Runs with normal unit tests: only the query-history boundary is mocked. Both parsers, the analyzer, and DataWave's document evaluator are real. No Accumulo
 * service is required. Run with {@code mvn -pl warehouse/query-core -am test -Dtest=QueryAnalyzerTest,QueryAnalyzerMockIntegrationTest
 * -Dsurefire.failIfNoSpecifiedTests=false -DskipFormat}.
 */
public class QueryAnalyzerMockIntegrationTest {
    interface QueryHistory {
        List<QueryInput> load();
    }

    @Test
    public void clustersHistoryAndEvaluatesQueriesOnMockDocuments() {
        QueryHistory history = mock(QueryHistory.class);
        when(history.load()).thenReturn(Arrays.asList(new QueryInput("NAME:alice", Syntax.LUCENE), new QueryInput("NAME == 'alice'", Syntax.JEXL),
                        new QueryInput("NAME:al*", Syntax.LUCENE), new QueryInput("NAME =~ 'al.*'", Syntax.JEXL),
                        new QueryInput("CITY:paris AND NAME:al*", Syntax.LUCENE), new QueryInput("NAME =~ 'al.*' && CITY == 'paris'", Syntax.JEXL),
                        new QueryInput("AGE:[20 TO 40]", Syntax.LUCENE), new QueryInput("((_Bounded_ = true) && (AGE >= '20' && AGE <= '40'))", Syntax.JEXL),
                        new QueryInput("#INCLUDE(NAME, 'al.*')", Syntax.LUCENE), new QueryInput("filter:includeRegex(NAME, 'al.*')", Syntax.JEXL),
                        new QueryInput("CITY:paris OR NAME:bob", Syntax.LUCENE), new QueryInput("NAME == 'bob' || CITY == 'paris'", Syntax.JEXL),
                        new QueryInput("NAME ==", Syntax.JEXL)));

        AnalysisReport report = new QueryAnalyzer().analyze(history.load());
        verify(history).load();
        assertEquals(13, report.getQueries().size());
        assertEquals(Status.INVALID, report.getQueries().get(12).getStatus());
        assertEquals(6, report.getClusters().size());

        List<List<Integer>> expectedHits = Arrays.asList(Arrays.asList(0), Arrays.asList(0, 2), Arrays.asList(0, 2), Arrays.asList(0, 2), Arrays.asList(0, 2),
                        Arrays.asList(0, 1, 2));
        List<Category> expectedCategories = Arrays.asList(Category.EQUALITY, Category.REGEX, Category.CONJUNCTION, Category.RANGE, Category.FUNCTION,
                        Category.DISJUNCTION);
        for (int pair = 0; pair < 6; pair++) {
            QueryAnalysis lucene = report.getQueries().get(pair * 2);
            QueryAnalysis jexl = report.getQueries().get(pair * 2 + 1);
            assertEquals(lucene.getError(), Status.SUCCESS, lucene.getStatus());
            assertEquals(jexl.getError(), Status.SUCCESS, jexl.getStatus());
            assertEquals(lucene.getSignature(), jexl.getSignature());
            assertEquals(2, report.getClusters().get(lucene.getSignature()).size());
            assertTrue(lucene.getCategories().contains(expectedCategories.get(pair)));
            assertTrue(jexl.getCategories().contains(expectedCategories.get(pair)));
            assertEquals(expectedHits.get(pair), evaluate(lucene));
            assertEquals(expectedHits.get(pair), evaluate(jexl));
        }
    }

    @Test
    public void sameClusterCanHaveDifferentEvaluationResults() {
        AnalysisReport report = new QueryAnalyzer().analyze(Arrays.asList("NAME == 'alice'", "NAME == 'bob'"), Syntax.JEXL);
        assertEquals(1, report.getClusters().size());
        assertEquals(Arrays.asList(0), evaluate(report.getQueries().get(0)));
        assertEquals(Arrays.asList(1), evaluate(report.getQueries().get(1)));
    }

    @Test
    public void clustersAndDrainsComplexSampleHistoryWithActualEvaluation() throws Exception {
        List<QueryInput> inputs = new ArrayList<>();
        List<String> families = new ArrayList<>();
        List<List<Integer>> hits = new ArrayList<>();
        try (InputStream stream = getClass().getResourceAsStream("/datawave/query/analysis/complex-query-workload.tsv")) {
            assertTrue("Sample workload resource missing", stream != null);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                reader.readLine();
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] columns = line.split("\t", -1);
                    assertEquals(5, columns.length);
                    inputs.add(new QueryInput(columns[3], Syntax.valueOf(columns[1])));
                    families.add(columns[2]);
                    List<Integer> expected = null;
                    if (!"-".equals(columns[4])) {
                        expected = new ArrayList<>();
                        for (String hit : columns[4].split(",")) {
                            expected.add(Integer.parseInt(hit));
                        }
                    }
                    hits.add(expected);
                }
            }
        }
        QueryHistory history = mock(QueryHistory.class);
        when(history.load()).thenReturn(inputs);
        AnalysisReport report = new QueryAnalyzer().analyze(history.load());
        verify(history).load();
        QueryClusterer.Result groups = new QueryClusterer().cluster(report);
        for (QueryAnalysis query : report.getQueries()) {
            assertEquals(query.getInput().getQuery() + ": " + query.getError(),
                            "invalid".equals(families.get(query.getIndex())) ? Status.INVALID : Status.SUCCESS, query.getStatus());
        }
        QueryClusteringTest.assertMembership(groups);
        assertEquals(1, groups.getExcludedCount());
        Map<String,String> familyToGroup = new HashMap<>();
        for (QueryClusterer.Group group : groups.getGroups()) {
            Set<String> groupFamilies = new HashSet<>();
            for (QueryAnalysis member : group.getMembers()) {
                String family = families.get(member.getIndex());
                groupFamilies.add(family);
                String existing = familyToGroup.putIfAbsent(family, group.getId());
                assertTrue("Split expected family " + family, existing == null || existing.equals(group.getId()));
            }
            assertEquals("Merged different sample families", 1, groupFamilies.size());
        }
        assertEquals(new HashSet<>(families).size() - 1, groups.getGroups().size());
        List<Selection> selections = new QueryWorkloadSelector().cursor(groups).take(Integer.MAX_VALUE);
        assertEquals(inputs.size() - 2, selections.size()); // one invalid entry and one repeated Lucene input
        assertEquals(inputs.size() - 1, selections.stream().mapToInt(Selection::getOccurrenceCount).sum());
        QueryWorkloadSelectorTest.assertFairRounds(groups, selections);
        for (Selection selection : selections) {
            int index = selection.getQuery().getIndex();
            if (hits.get(index) != null) {
                assertEquals(inputs.get(index).getQuery(), hits.get(index), evaluate(selection.getQuery()));
            }
        }
    }

    private List<Integer> evaluate(QueryAnalysis query) {
        List<Document> documents = Arrays.asList(document("alice", "paris", "30"), document("bob", "rome", "50"), document("alicia", "paris", "25"));
        List<Integer> matches = new ArrayList<>();
        JexlEvaluation evaluation = new JexlEvaluation(query.getJexl());
        for (int index = 0; index < documents.size(); index++) {
            Document document = documents.get(index);
            DatawaveJexlContext context = new DatawaveJexlContext();
            document.visit(query.getFields(), context);
            if (evaluation.apply(new Tuple3<>(new Key("shard", "datatype\0" + index), document, context))) {
                matches.add(index);
            }
        }
        return matches;
    }

    private Document document(String name, String city, String age) {
        Key key = new Key("shard", "datatype\0" + name);
        Document document = new Document();
        document.put("NAME", new Content(name, key, true));
        document.put("CITY", new Content(city, key, true));
        document.put("AGE", new Content(age, key, true));
        return document;
    }
}
