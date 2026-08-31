package datawave.query;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.Sets;

import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.CommonalityTokenTestDataIngest;
import datawave.table.constants.TableName;

/**
 * Tests the limit.fields feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query. This test uses a dot delimited token in the event field name as a 'commonality token'
 *
 */
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
public class HitsAreAlwaysIncludedCommonalityTokenTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(HitsAreAlwaysIncludedCommonalityTokenTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private Set<String> expectedResults = new HashSet<>();

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
    }

    @Override
    protected void extraAssertions() {
        assertFalse(results.isEmpty(), "No docs were returned!");

        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // local copy rather than destructively consuming the shared expectedResults set.
        Set<String> remaining = new HashSet<>(expectedResults);

        for (Document d : results) {
            log.trace(d);

            Attribute<?> hitAttribute = d.get(JexlEvaluation.HIT_TERM_FIELD);
            if (hitAttribute instanceof Attributes) {
                Attributes attributes = (Attributes) hitAttribute;
                for (Attribute<?> attr : attributes.getAttributes()) {
                    if (attr instanceof Content) {
                        Content content = (Content) attr;
                        assertTrue(remaining.contains(content.getContent()));
                    }
                }
            } else if (hitAttribute instanceof Content) {
                Content content = (Content) hitAttribute;
                assertTrue(remaining.contains(content.getContent()));
            }

            // remove from remaining as we find the expected return fields
            log.debug("remaining: " + remaining);
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();
            log.debug("dictionary:" + dictionary);
            for (Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {

                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute<?> attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        boolean found = remaining.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else
                            log.debug("Did not remove " + toFind);
                    }
                } else {
                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();

                    boolean found = remaining.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else
                        log.debug("Did not remove " + toFind);
                }
            }

            assertTrue(remaining.isEmpty(), remaining + " was not empty");
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(HitsAreAlwaysIncludedCommonalityTokenTest.class.toString(), log);
        clientForTest = qtth.client;

        // ingest with the document range only; CommonalityTokenTestDataIngest already uses IndexIngestUtil
        // internally to derive the other shard index table variants that AbstractQueryTest iterates over.
        CommonalityTokenTestDataIngest.writeItAll(clientForTest, CommonalityTokenTestDataIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(clientForTest, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup() {
        setClientForTest(clientForTest);
        logic.setCollapseUids(false);

        givenDate("20091231", "20150101");
    }

    private void runTestQuery(String queryString, Map<String,String> extraParms, Set<String> goodResults) throws Exception {
        this.expectedResults = goodResults;
        givenQuery(queryString);
        givenParameters(extraParms);

        planAndExecuteQuery();
    }

    @Test
    public void testWhereTheWildThingsAre() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=2");

        String queryString = "BIRD == 'buzzard'";

        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.3:buzzard", "CAT.WILD.3:puma", "CANINE.WILD.3:dingo", "FISH.WILD.3:salmon");

        runTestQuery(queryString, extraParameters, goodResults);
    }

    @Test
    public void testPetSounds() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=2");

        String queryString = "FISH == 'angelfish'";

        Set<String> goodResults = Sets.newHashSet("BIRD.PET.2:parrot", "CAT.PET.2:tom", "CANINE.PET.2:chihuahua", "FISH.PET.2:angelfish");

        runTestQuery(queryString, extraParameters, goodResults);
    }

}
