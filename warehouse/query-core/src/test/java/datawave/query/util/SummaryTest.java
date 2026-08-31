package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.helpers.PrintUtility;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;
import datawave.query.util.WiseGuysIngest.WhatKindaRange;
import datawave.table.constants.TableName;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class SummaryTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(SummaryTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic eventLogic;

    @Autowired
    @Qualifier("TLDEventQuery")
    protected ShardQueryLogic TLDLogic;

    private ShardQueryLogic currentLogic;
    private final Map<String,String> extraParameters = new HashMap<>();
    private final Set<String> goodResults = new HashSet<>();
    private boolean shouldReturnSomething;

    @Override
    public ShardQueryLogic getLogic() {
        return currentLogic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
        givenParameters(extraParameters);
    }

    @Override
    protected void extraAssertions() {
        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so check
        // against a local copy rather than destructively consuming the shared goodResults set.
        Set<String> remaining = new HashSet<>(goodResults);
        Set<String> unexpectedFields = new HashSet<>();
        for (Document d : results) {
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();

            log.debug("dictionary:" + dictionary);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {
                String fieldName = dictionaryEntry.getKey();

                // skip expected generated fields
                if (fieldName.equals(JexlEvaluation.HIT_TERM_FIELD) || fieldName.contains("ORIGINAL_COUNT") || fieldName.equals("RECORD_ID")
                                || (currentLogic instanceof TLDQueryLogic && fieldName.equals("QUOTE"))) {
                    continue;
                }

                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute<?> attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = fieldName + ":" + attr.getData();
                        boolean found = remaining.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else {
                            unexpectedFields.add(toFind);
                        }
                    }
                } else {
                    String toFind = fieldName + ":" + attribute.getData();

                    boolean found = remaining.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else {
                        unexpectedFields.add(toFind);
                    }
                }
            }
        }

        assertTrue(unexpectedFields.isEmpty(), "unexpected fields returned: " + unexpectedFields);
        assertTrue(remaining.isEmpty(), remaining + " was not empty");

        // AbstractQueryTest always forces hitList=true, so even a "nothing should match" case (e.g. a
        // malformed #SUMMARY() option) can still surface a bare HIT_TERM-only document; that field is
        // already filtered out and checked above, so only require non-emptiness for the positive case.
        if (shouldReturnSomething) {
            assertFalse(results.isEmpty(), "No docs were returned!");
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(SummaryTest.class.toString(), log);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    private void runTestQuery(String queryString, Map<String,String> extraParams, Set<String> expectedGoodResults, boolean shouldReturnSomething,
                    ShardQueryLogic logic) throws Exception {
        setClientForTest(client);
        this.currentLogic = logic;

        this.extraParameters.clear();
        this.extraParameters.putAll(extraParams);
        this.goodResults.clear();
        this.goodResults.addAll(expectedGoodResults);
        this.shouldReturnSomething = shouldReturnSomething;

        givenDate("20121231", "20130102");
        givenQuery(queryString);

        planAndExecuteQuery();
    }

    // TODO: remove @Disabled after we can except no argument in function
    @Disabled
    @Test
    public void testWithNoArg() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY()";

        Set<String> goodResults = new HashSet<>(
                        Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testWithNoActualArg() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(/hello&%526++/@?Sy-;xtVrxHN;%)";

        Set<String> goodResults = new HashSet<>(
                        Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testWithOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(VIEWS:CONTENT/SIZE:50/ONLY)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gu"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testWithoutOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gu"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gu"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testOverMaxSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:90000)";

        Set<String> goodResults = new HashSet<>(
                        Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testNegativeSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:-50)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:CONTENT: Y"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testNoContentFound() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/ONLY/VIEWS:CANTFINDME,ORME)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:NO CONTENT FOUND TO SUMMARIZE"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testSizeZero() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:0)";

        Set<String> goodResults = Collections.emptySet();
        runTestQuery(queryString, extraParameters, goodResults, false, eventLogic);
    }

    @Test
    public void testNoSizeButOtherOptions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(VIEWS:TEST1,TEST2)";

        Set<String> goodResults = new HashSet<>(
                        Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testBadOptionsFormat() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:notanumber)";

        Set<String> goodResults = Collections.emptySet();

        runTestQuery(queryString, extraParameters, goodResults, false, eventLogic);
    }

    @Test
    public void testOnlyWithNoOtherOptions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(ONLY)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:NO CONTENT FOUND TO SUMMARIZE"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testMultiView() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT*/ONLY)";

        Set<String> goodResults = new HashSet<>(Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gu"
                        + "\nCONTENT2: A lawyer and his briefcase can steal more than ten"));

        runTestQuery(queryString, extraParameters, goodResults, true, eventLogic);
    }

    @Test
    public void testWithTLD() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:farther AND QUOTE:child #SUMMARY(gimme)";

        Set<String> goodResults = new HashSet<>(
                        Set.of("SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone"));

        runTestQuery(queryString, extraParameters, goodResults, true, TLDLogic);
    }
}
