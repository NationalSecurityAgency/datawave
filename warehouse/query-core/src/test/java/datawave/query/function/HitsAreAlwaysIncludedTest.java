package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.Sets;

import datawave.data.type.DateType;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.LimitFieldsTestingIngest;
import datawave.table.constants.TableName;

/**
 * Tests the {@code limit.fields} feature to ensure that hit terms are always included and that associated fields at the same grouping context are included
 * along with the field that hit on the query
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
public class HitsAreAlwaysIncludedTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(HitsAreAlwaysIncludedTest.class);
    private static final Authorizations auths = new Authorizations("ALL");
    // Under certain conditions a Date-normalized field value is returned without normalization.
    private static final Set<String> dateFields = Set.of("FOO_1_BAR_1.FOO.0", "FOO_1_BAR_1");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @TempDir
    Path tempDir;

    private final Set<String> expectedHits = new HashSet<>();
    private final Set<String> expectedEntries = new HashSet<>();

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
        Document document = results.iterator().next();
        assertHits(document);
        assertEntries(document);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(HitsAreAlwaysIncludedTest.class.toString(), log);
        clientForTest = qtth.client;

        // ingest with the document range only; LimitFieldsTestingIngest already uses IndexIngestUtil
        // internally to derive the other shard index table variants (NO_UID_INDEX, TRUNCATED_INDEX, etc.)
        // that AbstractQueryTest.planAndExecuteQuery() iterates over.
        LimitFieldsTestingIngest.writeItAll(clientForTest, LimitFieldsTestingIngest.WhatKindaRange.DOCUMENT);
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
        logic.setFullTableScanEnabled(true);
        logic.setCollapseUids(false);

        givenDate("20091231", "20150101");
        expectResultCount(1);
        expectedHits.clear();
        expectedEntries.clear();
    }

    private void expectEntry(String entry) {
        this.expectedEntries.add(entry);
    }

    private void expectHit(String hit) {
        this.expectedHits.add(hit);
    }

    private void assertHits(Document document) {
        log.debug("Expected hits: " + document);

        if (expectedHits.isEmpty()) {
            return;
        }

        Attribute<?> hitAttribute = document.get(JexlEvaluation.HIT_TERM_FIELD);
        assertNotNull(hitAttribute, "Did not find hit term field " + JexlEvaluation.HIT_TERM_FIELD);

        Set<String> hits = getContents(hitAttribute);

        Set<String> missingHits = Sets.difference(expectedHits, hits);
        assertTrue(missingHits.isEmpty(), "Expected hits missing: " + missingHits);

        Set<String> unexpectedHits = Sets.difference(hits, expectedHits);
        assertTrue(unexpectedHits.isEmpty(), "Unexpected hits found: " + unexpectedHits);
    }

    private static Set<String> getContents(Attribute<?> hitAttribute) {
        Set<String> hits = Sets.newHashSet();
        if (hitAttribute instanceof Attributes) {
            Attributes hitAttributes = (Attributes) hitAttribute;
            for (Attribute<?> attribute : hitAttributes.getAttributes()) {
                if (attribute instanceof Content) {
                    Content content = (Content) attribute;
                    hits.add(content.getContent());
                }
            }
        } else if (hitAttribute instanceof Content) {
            Content content = (Content) hitAttribute;
            hits.add(content.getContent());
        }
        return hits;
    }

    private void assertEntries(Document document) {
        log.debug("Expected entries: " + expectedEntries);
        Map<String,Attribute<? extends Comparable<?>>> dictionary = document.getDictionary();
        log.debug("Dictionary: " + dictionary);

        Set<String> entries = Sets.newHashSet();
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dictionary.entrySet()) {
            String key = entry.getKey();

            // Ignore the hit term field and record id.
            if (key.equals(JexlEvaluation.HIT_TERM_FIELD) || key.equals(Document.DOCKEY_FIELD_NAME)) {
                continue;
            }

            Attribute<? extends Comparable<?>> attribute = entry.getValue();
            if (attribute instanceof Attributes) {
                Attributes attributes = (Attributes) attribute;
                for (Attribute<?> attr : attributes.getAttributes()) {
                    if (!key.endsWith(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                        entries.add(key + ":" + attr);
                    }
                }
            } else {
                if (dateFields.contains(key)) {
                    DateType dateType = new DateType(attribute.getData().toString());
                    entries.add(key + ":" + dateType.getNormalizedValue());
                } else if (!key.endsWith(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                    entries.add(key + ":" + attribute);
                }
            }
        }

        Set<String> missingEntries = Sets.difference(expectedEntries, entries);
        assertTrue(missingEntries.isEmpty(), "Expected entries missing: " + missingEntries);

        Set<String> unexpectedEntries = Sets.difference(entries, expectedEntries);
        assertTrue(unexpectedEntries.isEmpty(), "Unexpected entries found: " + unexpectedEntries);
    }

    @Test
    public void testHitForIndexedQueryTerm() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testHitForIndexedQueryTermWithOptionsInQueryFunction() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and f:options('include.grouping.context', 'true', "
                        + "'hit.list', 'true', 'limit.fields', 'FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0')");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        givenQuery("FOO_3 == 'defg'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3.FOO.3.3:defg");

        planAndExecuteQuery();
    }

    @Test
    public void testHitForIndexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1.FOO.1.3:good");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");
        expectHit("FOO_1.FOO.1.3:good");

        planAndExecuteQuery();
    }

    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        // the hit
        expectEntry("FOO_3_BAR:defg<cat>");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        expectHit("FOO_3_BAR:defg<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testHitWithRange() throws Exception {
        givenQuery("((_Bounded_ = true) && (FOO_1_BAR_1 >= '2021-03-01 00:00:00' && FOO_1_BAR_1 <= '2021-04-01 00:00:00'))");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");

        // the hit
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_3_BAR:bcde<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        planAndExecuteQuery();
    }

    @Test
    public void testHitWithDate() throws Exception {
        givenQuery("FOO_1_BAR_1 == '2021-03-24T16:00:00.000Z'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");

        // the hit
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_3_BAR:bcde<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        planAndExecuteQuery();
    }

    @Test
    public void testHitWithExceededOrThreshold() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=1,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        // the hits
        expectEntry("FOO_3_BAR:defg<cat>");
        expectEntry("FOO_3_BAR:abcd<cat>");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        expectHit("FOO_3_BAR:defg<cat>");
        expectHit("FOO_3_BAR:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testHitsOnly() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectEntry("FOO_3_BAR:defg<cat>");
        expectEntry("FOO_3_BAR:abcd<cat>");

        expectHit("FOO_3_BAR:defg<cat>");
        expectHit("FOO_3_BAR:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testGroupedHitsOnly() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.3:good");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");
        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingField() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenParameter("matching.field.sets", "FOO_4");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingFields() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenParameter("matching.field.sets", "FOO_4=BAR_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testGroupedHitsWithMoreMatchingFields() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenParameter("matching.field.sets", "FOO_4=BAR_1=FOO_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.2:yawn");
        expectEntry("BAR_2.BAR.2.2:siberian");
        expectEntry("BAR_3.BAR.3.2:pink");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        planAndExecuteQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingFieldSets() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenParameter("matching.field.sets", "FOO_4=BAR_1,FOO_1=BAR_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.2:yawn");
        expectEntry("BAR_2.BAR.2.2:siberian");
        expectEntry("BAR_3.BAR.3.2:pink");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        planAndExecuteQuery();
    }

    protected void ivaratorConfig() throws IOException {
        final URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        assertNotNull(hdfsConfig, "Failed to fetch testhadoop.config URL");
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());

        final List<String> dirs = new ArrayList<>();
        Path ivCache = tempDir.resolve("ivarator-" + UUID.randomUUID());
        Files.createDirectories(ivCache);
        dirs.add(ivCache.toUri().toString());
        String uriList = String.join(",", dirs);
        log.debug("hdfs dirs(" + uriList + ")");
        this.logic.setIvaratorCacheDirConfigs(dirs.stream().map(IvaratorCacheDirConfig::new).collect(Collectors.toList()));
    }

}
