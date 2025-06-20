package datawave.query;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.helpers.PrintUtility;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
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
public class ExcerptTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(ExcerptTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private final Map<String,String> extraParameters = new HashMap<>();
    private final Set<String> expectedResults = new HashSet<>();

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
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
        // planAndExecuteQuery() invokes extraAssertions() once per index table variant (see
        // AbstractQueryTest#getIndexTableNames()), so check against a local copy rather than
        // destructively consuming the shared expectedResults set.
        Set<String> remaining = new HashSet<>(expectedResults);
        Set<String> unexpectedFields = new HashSet<>();
        for (Document d : results) {
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();

            log.debug("dictionary:" + dictionary);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {
                String fieldName = dictionaryEntry.getKey();

                // skip expected generated fields
                if (fieldName.equals(JexlEvaluation.HIT_TERM_FIELD) || fieldName.contains("ORIGINAL_COUNT") || fieldName.equals("RECORD_ID")) {
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
        assertFalse(results.isEmpty(), "No docs were returned!");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(ExcerptTest.class.toString(), log);
        client = qtth.client;
        WiseGuysIngest.writeItAll(client, WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void setupLogic() {
        setClientForTest(client);

        givenDate("20121231", "20130102");

        extraParameters.clear();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        expectedResults.clear();
    }

    private void updateQueryParam(String key, String value) {
        if (!key.isBlank() && !value.isBlank()) {
            extraParameters.put(key, value);
        }
    }

    private void addExpectedResult(String result) {
        if (!result.isBlank()) {
            expectedResults.add(result);
        }
    }

    private void runTestQuery(String queryString) throws Exception {
        givenQuery(queryString);
        planAndExecuteQuery();
    }

    @Test
    public void simpleTest() throws Exception {
        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2)";

        // not sure why the timestamp and delete flag are present
        addExpectedResult("HIT_EXCERPT:get much [farther] with a");

        runTestQuery(queryString);
    }

    @Test
    public void simpleTestBefore() throws Exception {
        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2/before)";

        // not sure why the timestamp and delete flag are present
        addExpectedResult("HIT_EXCERPT:get much [farther]");

        runTestQuery(queryString);
    }

    @Test
    public void simpleTestAfter() throws Exception {
        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2/after)";

        // not sure why the timestamp and delete flag are present
        addExpectedResult("HIT_EXCERPT:[farther] with a");

        runTestQuery(queryString);
    }

    @Test
    public void lessSimpleBeforeTest() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2/before)";

        addExpectedResult("HIT_EXCERPT:an offer [he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void lessSimpleAfterTest() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2/after)";

        addExpectedResult("HIT_EXCERPT:[he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void lessSimpleTest() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2)";

        addExpectedResult("HIT_EXCERPT:an offer [he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void biggerRangeThanQuoteLength() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20)";

        addExpectedResult("HIT_EXCERPT:im gonna make him an offer [he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void biggerRangeThanQuoteLengthBeforeTest() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20/before)";

        addExpectedResult("HIT_EXCERPT:im gonna make him an offer [he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void biggerRangeThanQuoteLengthAfterTest() throws Exception {
        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20/after)";

        addExpectedResult("HIT_EXCERPT:[he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void wholeQuote() throws Exception {
        String queryString = "QUOTE:(im gonna make him an offer he cant refuse) #EXCERPT_FIELDS(QUOTE/20)";

        addExpectedResult("HIT_EXCERPT:[im] [gonna] [make] [him] [an] [offer] [he] [cant] [refuse]");

        runTestQuery(queryString);
    }

    @Test
    public void anotherFirstTerm() throws Exception {
        updateQueryParam("return.fields", "HIT_EXCERPT,UUID");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3)";

        addExpectedResult("UUID.0:SOPRANO");
        addExpectedResult("HIT_EXCERPT:[if] you can quote");

        runTestQuery(queryString);
    }

    @Test
    public void anotherFirstTermBeforeTest() throws Exception {
        updateQueryParam("return.fields", "HIT_EXCERPT,UUID");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3/before)";

        addExpectedResult("UUID.0:SOPRANO");
        addExpectedResult("HIT_EXCERPT:[if]");

        runTestQuery(queryString);
    }

    @Test
    public void anotherFirstTermAfterTest() throws Exception {
        updateQueryParam("return.fields", "HIT_EXCERPT,UUID");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3/after)";

        addExpectedResult("UUID.0:SOPRANO");
        addExpectedResult("HIT_EXCERPT:[if] you can quote");

        runTestQuery(queryString);
    }
}
